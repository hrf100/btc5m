#!/usr/bin/env python3
"""
btc5m v2 —— Polymarket BTC 5分钟涨跌盘: 场次预判 + 场中公允概率 + 数据采集 + Web看板(9999)

结算规则(2026-08-14起): Chainlink BTC/USD 60秒TWAP
    Price to beat P0 = 开盘时刻 TWAP60 = 开盘前60秒均价
    结算价 C         = 收盘时刻 TWAP60 = 最后60秒均价
    C >= P0 => Up (平局算Up)
=> 只要知道已发生的价格, 就能算出 P(C>=P0) 的公允概率; 越接近收盘越确定。

环境变量（写在服务器 ~/.btc5m-4.env ，不要放进仓库）:
    POLY_PRIVATE_KEY  钱包私钥。启动时自动换取 API 凭据，不必手填三项
    POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE  已经有凭据时直接用
    DASH_TOKEN  可选，设置后访问看板需带 ?token=xxx
服务器一行启动:
  curl -fsSL https://raw.githubusercontent.com/hrf100/btc5m/main/test4/run.sh | bash
"""
import asyncio, json, time, math, csv, os, logging, argparse
from bisect import bisect_left, bisect_right
from collections import deque
import aiohttp
from aiohttp import web

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
POLY_WS = "wss://ws-live-v2.polymarket.com/ws"
BN_WS = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth10@100ms"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"

WIN, TW = 300, 60
LEAD = 20                 # 开盘前多少秒做预判
CONF_TH = 0.05            # |p-0.5| 超过此值算"有信心"
DEFAULT_SIGMA = 6.0       # $/sqrt(s), 启动初期用
DATA = "data"
DASH_TOKEN = os.getenv("DASH_TOKEN", "")
T5 = aiohttp.ClientTimeout(total=5)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("btc5m")


# ------------------------------------------------------------------ 数据结构
class Series:
    """按时间递增的 (ts, px) 序列, 支持 bisect 查询"""

    def __init__(self, keep=1300):
        self.t, self.p, self.keep = [], [], keep

    def add(self, ts, px, gap=0.0):
        if self.t and ts < self.t[-1]:
            return
        if self.t and ts - self.t[-1] < gap:
            self.p[-1] = px
            return
        self.t.append(ts)
        self.p.append(px)
        if len(self.t) > self.keep + 400:
            i = bisect_left(self.t, ts - self.keep)
            del self.t[:i], self.p[:i]

    def at(self, t):
        i = bisect_right(self.t, t) - 1
        return self.p[i] if i >= 0 else None

    def last(self):
        return self.p[-1] if self.p else None


class St:
    def __init__(self):
        self.cl, self.tw, self.bn, self.okx = Series(), Series(), Series(), Series()
        self.flow = {"bn": deque(), "okx": deque()}
        self.obi = {"bn": 0.0, "okx": 0.0}
        self.last_msg = {k: 0 for k in ("cl", "tw", "bn", "okx", "pm")}
        self.cl_src = ""
        self.auth = "no-creds"
        self.pm, self.tokens, self.pm_ws = {}, set(), None
        self.win, self.rounds = {}, deque(maxlen=300)
        self.view, self.sigma = {}, DEFAULT_SIGMA
        self.conn_t = 0.0          # 最近一次PolyBolt连接时间
        self.cl_nofilter = False   # 看门狗触发后切换: 不带symbol过滤订阅, 本地过滤btcusd


S = St()


def cl_fresh(now=None):
    """Chainlink现货是否新鲜: 消息刚到达(<5s) 且 价格自带时间戳也够新(<10s)。
    只看到达时间会漏掉"消息还在来但价格是旧的/重复的"这种情况。"""
    now = now or time.time()
    return bool(S.cl_src == "chainlink" and S.cl.t and now - S.last_msg["cl"] < 5 and now - S.cl.t[-1] < 10)


def cl_near(t, tol=15.0):
    """t±tol 秒内是否有过Chainlink价格点 (用于判断场次开盘/收盘时数据是否有效)"""
    i = bisect_left(S.cl.t, t - tol)
    return i < len(S.cl.t) and S.cl.t[i] <= t + tol


def cl_stalled(now=None, limit=20.0):
    """已认证连接着, 但Chainlink价格流超过limit秒没有新点 -> 视为订阅静默/卡死"""
    now = now or time.time()
    if S.auth != "ok" or S.pm_ws is None or S.pm_ws.closed:
        return False
    if now - S.conn_t < limit:
        return False
    return (not S.cl.t) or now - S.cl.t[-1] > limit or now - S.last_msg["cl"] > limit


def get_ref():
    """结算用的参考价: Chainlink现货优先, 否则退回币安(代理)"""
    if cl_fresh():
        return S.cl, "chainlink"
    return S.bn, "binance*"


# ------------------------------------------------------------------ 公允概率模型
def _F(x, y):
    if x > y:
        x, y = y, x
    return x * x * y / 2 - x ** 3 / 6


def _G(a1, b1, a2, b2):
    """Cov(∫_{a1}^{b1} W, ∫_{a2}^{b2} W) / sigma^2, W为布朗运动"""
    return _F(b1, b2) - _F(a1, b2) - _F(b1, a2) + _F(a1, a2)


def fair(t, s0, ref, sigma, p0_fix=None, shift=0.0):
    """
    在时刻t, 估计 P(C >= P0)。
    P0 = 均值[s0-60, s0], C = 均值[s0+240, s0+300]。已发生部分用历史价, 未发生部分用当前价+布朗运动。
    sigma: 价格波动 $/sqrt(s)
    """
    e = s0 + WIN
    st = ref.at(t)
    if st is None:
        return None
    st += shift          # nowcast: 用币安领先量修正Chainlink的约2秒滞后

    def avg_part(a, b):
        kb = min(b, t)
        k = int(kb - a)
        known = 0.0
        if k > 0:
            known = sum((ref.at(a + i + 1) or st) for i in range(k)) * ((kb - a) / k)
        return known + st * max(0.0, b - max(a, t))

    p0 = avg_part(s0 - TW, s0) / TW
    c = avg_part(e - TW, e) / TW
    if p0_fix is not None and t >= s0:   # 开盘后P0已固定, 用官方TWAP值
        p0 = p0_fix
    mu = c - p0

    def rng(a, b):
        lo, hi = max(a, t) - t, b - t
        return (lo, hi) if hi > lo else None

    A, B = rng(e - TW, e), rng(s0 - TW, s0)
    v = 0.0
    if A:
        v += _G(*A, *A)
    if B:
        v += _G(*B, *B)
    if A and B:
        v -= 2 * _G(*A, *B)
    sd = sigma * math.sqrt(max(v, 0.0)) / TW
    if sd < 1e-9:
        p = 1.0 if mu >= 0 else 0.0
    else:
        p = 0.5 * (1 + math.erf(mu / (sd * math.sqrt(2))))
    return {"p": p, "mu": mu, "sd": sd, "p0": p0, "c": c}


CALIB = []


def load_calib():
    try:
        CALIB[:] = json.load(open(f"{DATA}/calib.json"))["buckets"]
        log.info("已加载校准参数 %d 档", len(CALIB))
    except Exception:
        pass


def calibrate(p, tau):
    """Platt校准: 按 tau 分档修正模型概率 (参数由 analyze.py 生成)"""
    for lo, hi, a, b in CALIB:
        if lo <= tau < hi:
            x = min(max(p, 1e-4), 1 - 1e-4)
            return 1 / (1 + math.exp(-(a * math.log(x / (1 - x)) + b)))
    return p


def official_p0(s0):
    """开盘时刻的官方 Chainlink TWAP60 (流里离s0最近且不晚于s0的点, 超过3秒视为没有)"""
    i = bisect_right(S.tw.t, s0) - 1
    return S.tw.p[i] if i >= 0 and s0 - S.tw.t[i] <= 3 else None


def est_sigma_h(h, now, lag=30, span=1200):
    """lag秒尺度的波动 ($/sqrt(s))。Chainlink现货方差比随周期上升(60秒约为2秒的1.67倍),
    逐tick估计会低估30-60秒波动约29%, 导致高概率区间过度自信。"""
    n, acc, t = 0, 0.0, now - span
    while t + lag <= now:
        a, b = h.at(t), h.at(t + lag)
        if a is not None and b is not None:
            acc += (b - a) ** 2
            n += 1
        t += 5.0
    return math.sqrt(acc / (n * lag)) if n >= 20 else None


def nowcast_shift(now, g=2.0):
    """币安领先Chainlink约2秒: 用 bn(now)-bn(now-g) 估计Chainlink即将出现的变动"""
    if not S.bn.t or now - S.bn.t[-1] > 3:
        return 0.0
    b0 = S.bn.at(now - g)
    return S.bn.p[-1] - b0 if b0 is not None else 0.0


def est_sigma(h, now, span=300):
    i0 = bisect_left(h.t, now - span)
    ts, ps = h.t[i0:], h.p[i0:]
    if len(ts) < 30:
        return None
    acc = tot = 0.0
    for k in range(1, len(ts)):
        dt = ts[k] - ts[k - 1]
        if dt > 0:
            acc += (ps[k] - ps[k - 1]) ** 2
            tot += dt
    return math.sqrt(acc / tot) if tot > 0 else None


def flow_ratio(dq, now, sec=60):
    while dq and dq[0][0] < now - 180:
        dq.popleft()
    xs = [q for ts, q in dq if ts >= now - sec]
    tot = sum(abs(x) for x in xs)
    return sum(xs) / tot if tot > 0 else 0.0


def features(t):
    """辅助特征(仅记录, 用于后续拟合)"""
    f, bn = {}, S.bn.last()
    for k, sec in (("r1", 60), ("r3", 180), ("r5", 300), ("r15", 900)):
        p0 = S.bn.at(t - sec)
        f[k] = (bn / p0 - 1) * 1e4 if bn and p0 else 0.0
    f["flow"] = (flow_ratio(S.flow["bn"], t) + flow_ratio(S.flow["okx"], t)) / 2
    f["obi"] = (S.obi["bn"] + S.obi["okx"]) / 2
    cl, okx = S.cl.last(), S.okx.last()
    f["bn_cl_bps"] = (bn / cl - 1) * 1e4 if bn and cl else 0.0
    f["bn_okx_bps"] = (bn / okx - 1) * 1e4 if bn and okx else 0.0
    return {k: round(v, 4) for k, v in f.items()}


# ------------------------------------------------------------------ Polymarket 数据
def fl(x):
    try:
        return float(x)
    except Exception:
        return None


def quote(tok):
    q = S.pm.get(tok)
    return (q[0], q[1]) if q else (None, None)


async def pm_sub(tokens, on=True):
    for t in tokens:
        (S.tokens.add if on else S.tokens.discard)(t)
    ws = S.pm_ws
    if ws is not None and not ws.closed and tokens:
        await ws.send_json({"op": "subscribe" if on else "unsubscribe",
                            "subscriptions": [{"channel": "price.polymarket", "filter": {"asset_id": t}}
                                              for t in tokens]})


def _creds():
    c = (os.getenv("POLY_API_KEY"), os.getenv("POLY_API_SECRET"), os.getenv("POLY_API_PASSPHRASE"))
    return c if all(c) else None


def _g(o, *names):
    for n in names:
        v = getattr(o, n, None)
        if v is None and isinstance(o, dict):
            v = o.get(n)
        if v:
            return v
    return None


_last_derive = [0.0]


async def maybe_derive():
    """没有 POLY_API_* 但有 POLY_PRIVATE_KEY 时, 自动生成API凭据(只放内存, 不落盘)"""
    c = _creds()
    pk = os.getenv("POLY_PRIVATE_KEY")
    if c or not pk:
        return c
    if time.time() - _last_derive[0] < 60:
        return None
    _last_derive[0] = time.time()
    S.auth = "deriving"

    def work():
        from py_clob_client_v2 import ClobClient
        cr = ClobClient(host=CLOB, chain_id=137, key=pk).create_or_derive_api_key()
        return _g(cr, "api_key", "apiKey", "key"), _g(cr, "api_secret", "secret"), _g(cr, "api_passphrase", "passphrase")

    try:
        k, sec, pp = await asyncio.to_thread(work)
        if k and sec and pp:
            os.environ["POLY_API_KEY"], os.environ["POLY_API_SECRET"], os.environ["POLY_API_PASSPHRASE"] = k, sec, pp
            log.info("已用私钥自动生成 Polymarket API 凭据 (仅内存, 未落盘)")
            return (k, sec, pp)
        log.warning("自动生成API凭据: SDK返回内容不完整")
    except ImportError:
        log.warning("缺少 py-clob-client-v2: 请运行 /opt/btc5m/venv/bin/pip install py-clob-client-v2")
    except Exception as e:
        log.warning("自动生成API凭据失败: %s: %s", type(e).__name__, str(e).replace(pk, "***")[:200])
    S.auth = "derive-failed"
    return None


def _crypto_points(pl):
    if not isinstance(pl, dict) or pl.get("symbol") != "btcusd":
        return []
    pts = pl["data"] if isinstance(pl.get("data"), list) else [pl]
    out = []
    for p in pts:
        v = fl(p.get("full_accuracy_value", p.get("value")))
        if v is not None and "timestamp" in p:
            out.append((p["timestamp"] / 1000, v))
    return out


async def ws_poly(sess):
    while True:
        creds = await maybe_derive()
        try:
            async with sess.ws_connect(POLY_WS, heartbeat=None) as ws:
                S.pm_ws = ws
                S.conn_t = time.time()
                gated = [{"channel": "price.crypto"} if S.cl_nofilter else
                         {"channel": "price.crypto", "filter": {"symbol": "btcusd"}},
                         {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}}]
                if S.cl_nofilter:
                    log.info("price.crypto 使用不带过滤的订阅, 本地只保留 btcusd")
                if creds:
                    S.auth = "authenticating"
                    await ws.send_json({"op": "auth", "rid": "a1", "auth": {
                        "apiKey": creds[0], "secret": creds[1], "passphrase": creds[2]}})
                else:
                    log.warning("没有 POLY_PRIVATE_KEY / POLY_API_* : 没有Chainlink价格, 退回币安代理")
                subs = [{"channel": "price.polymarket", "filter": {"asset_id": t}} for t in S.tokens]
                if subs:
                    await ws.send_json({"op": "subscribe", "rid": "s1", "subscriptions": subs})
                async for m in ws:
                    if m.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        d = json.loads(m.data)
                    except Exception:
                        continue
                    now = time.time()
                    op = d.get("op")
                    if op:
                        if op == "authed":
                            S.auth = "ok"
                            await ws.send_json({"op": "subscribe", "rid": "s2", "subscriptions": gated})
                        elif op == "error":
                            log.warning("polybolt error: %s", d)
                            if str(d.get("code", "")).startswith("auth"):
                                S.auth = "FAILED:" + d["code"]
                        continue
                    ch, pl = d.get("channel"), d.get("payload")
                    if ch == "price.polymarket":
                        if isinstance(pl, dict) and "asset_id" in pl:
                            S.pm[pl["asset_id"]] = (fl(pl.get("best_bid")), fl(pl.get("best_ask")), now)
                            S.last_msg["pm"] = now
                    elif ch == "price.crypto":
                        for ts, v in _crypto_points(pl):
                            S.cl.add(ts, v)
                        S.cl_src = (pl or {}).get("source", "") if isinstance(pl, dict) else S.cl_src
                        S.last_msg["cl"] = now
                    elif ch == "price.crypto.twap":
                        for ts, v in _crypto_points(pl):
                            S.tw.add(ts, v)
                        S.last_msg["tw"] = now
                log.warning("polybolt closed code=%s", ws.close_code)
        except Exception as e:
            log.warning("polybolt ws: %s", e)
        S.pm_ws = None
        await asyncio.sleep(3)


async def get_tokens(sess, start):
    try:
        async with sess.get(f"{GAMMA}/markets", params={"slug": f"btc-updown-5m-{start}"}, timeout=T5) as r:
            m = (await r.json())[0]
        ids, outs = m["clobTokenIds"], m["outcomes"]
        ids = json.loads(ids) if isinstance(ids, str) else ids
        outs = json.loads(outs) if isinstance(outs, str) else outs
        d = {o.lower(): i for o, i in zip(outs, ids)}
        return d.get("up"), d.get("down")
    except Exception:
        return None, None


async def gamma_outcome(sess, start):
    """官方结果: market closed 且 outcomePrices 为 1/0"""
    try:
        async with sess.get(f"{GAMMA}/markets", params={"slug": f"btc-updown-5m-{start}"}, timeout=T5) as r:
            m = (await r.json())[0]
        outs, pr = m["outcomes"], m["outcomePrices"]
        outs = json.loads(outs) if isinstance(outs, str) else outs
        pr = [float(x) for x in (json.loads(pr) if isinstance(pr, str) else pr)]
        if (m.get("closed") or m.get("umaResolutionStatus") == "resolved") and max(pr) >= 0.99 and min(pr) <= 0.01:
            return outs[pr.index(max(pr))]
    except Exception:
        pass
    return None


async def gamma_debug(sess, start):
    """官方结果取不到时, 打印Gamma返回的关键字段以便排查"""
    try:
        async with sess.get(f"{GAMMA}/markets", params={"slug": f"btc-updown-5m-{start}"}, timeout=T5) as r:
            j = await r.json()
        if not j:
            return "empty list (slug不存在?)"
        m = j[0]
        return {k: m.get(k) for k in ("closed", "active", "archived", "umaResolutionStatus",
                                       "outcomePrices", "endDate", "resolvedBy") if k in m}
    except Exception as e:
        return f"error {type(e).__name__}: {e}"


async def rest_best(sess, token):
    try:
        async with sess.get(f"{CLOB}/book", params={"token_id": token}, timeout=T5) as r:
            j = await r.json()
        bids = [float(x["price"]) for x in j.get("bids", [])]
        asks = [float(x["price"]) for x in j.get("asks", [])]
        return (max(bids) if bids else None, min(asks) if asks else None)
    except Exception:
        return None, None


async def pm_fallback(sess):
    """流里一直没有的token, 用REST补一次"""
    while True:
        await asyncio.sleep(5)
        for w in list(S.win.values()):
            for tok in (w.get("up"), w.get("dn")):
                if tok and tok not in S.pm:
                    b, a = await rest_best(sess, tok)
                    if b is not None or a is not None:
                        S.pm[tok] = (b, a, time.time())


# ------------------------------------------------------------------ 币安 / 欧易
async def ws_binance(sess):
    while True:
        try:
            async with sess.ws_connect(BN_WS, heartbeat=20) as ws:
                async for m in ws:
                    if m.type != aiohttp.WSMsgType.TEXT:
                        continue
                    d = json.loads(m.data).get("data", {})
                    now = time.time()
                    if d.get("e") == "aggTrade":
                        px, q = float(d["p"]), float(d["q"])
                        S.flow["bn"].append((now, -q if d["m"] else q))
                        S.bn.add(now, px, gap=1.0)
                    elif "bids" in d:
                        b = sum(float(x[1]) for x in d["bids"])
                        a = sum(float(x[1]) for x in d["asks"])
                        S.obi["bn"] = (b - a) / (b + a) if b + a else 0.0
                    S.last_msg["bn"] = now
        except Exception as e:
            log.warning("binance ws: %s", e)
        await asyncio.sleep(3)


async def ws_okx(sess):
    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"},
                                       {"channel": "books5", "instId": "BTC-USDT"}]}

    async def ping(ws):
        while True:
            await asyncio.sleep(20)
            await ws.send_str("ping")

    while True:
        try:
            async with sess.ws_connect(OKX_WS) as ws:
                await ws.send_json(sub)
                pt = asyncio.create_task(ping(ws))
                try:
                    async for m in ws:
                        if m.type != aiohttp.WSMsgType.TEXT or m.data == "pong":
                            continue
                        d = json.loads(m.data)
                        ch = (d.get("arg") or {}).get("channel")
                        now = time.time()
                        for it in d.get("data", []):
                            if ch == "trades":
                                px, q = float(it["px"]), float(it["sz"])
                                S.flow["okx"].append((now, q if it["side"] == "buy" else -q))
                                S.okx.add(now, px, gap=1.0)
                            elif ch == "books5":
                                b = sum(float(x[1]) for x in it["bids"])
                                a = sum(float(x[1]) for x in it["asks"])
                                S.obi["okx"] = (b - a) / (b + a) if b + a else 0.0
                        S.last_msg["okx"] = now
                finally:
                    pt.cancel()
        except Exception as e:
            log.warning("okx ws: %s", e)
        await asyncio.sleep(3)


# ------------------------------------------------------------------ 场次生命周期
_hdr_checked = set()


def append_csv(path, row):
    os.makedirs(DATA, exist_ok=True)
    if path not in _hdr_checked:   # 程序升级后列变了: 旧文件改名归档, 避免新数据列错位
        _hdr_checked.add(path)
        if os.path.exists(path):
            with open(path) as fh:
                first = fh.readline().strip().split(",")
            if first != list(row):
                bak = f"{path}.{int(time.time())}.bak"
                os.rename(path, bak)
                log.info("%s 表头已变化, 旧文件归档为 %s", path, bak)
    new = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


async def init_window(sess, s):
    w = S.win[s]
    while time.time() < s + WIN and s in S.win:
        up, dn = await get_tokens(sess, s)
        if up and dn:
            w["up"], w["dn"] = up, dn
            await pm_sub([up, dn], True)
            return
        await asyncio.sleep(3)


def make_pred(s):
    w = S.win[s]
    w["pred_done"] = True
    ref, rn = get_ref()
    if len(S.bn.t) < 120 or ref.last() is None:
        log.info("next %s: 历史数据不足, 跳过预判", s)
        return
    r = fair(time.time(), s, ref, S.sigma, shift=nowcast_shift(time.time()) if rn == "chainlink" else 0.0)
    if not r:
        return
    p = calibrate(r["p"], time.time() - s)
    w.update(pred="Up" if p >= 0.5 else "Down", p_pred=p, conf=abs(p - 0.5) >= CONF_TH,
             f=features(time.time()), r_pred=r, ref_name=rn,
             q0=quote(w["up"]) + quote(w["dn"]))
    log.info("next %s: pred=%s p_up=%.3f (spot-P0估计 %+.1f$, sd %.1f$) ref=%s up=%s dn=%s",
             s, w["pred"], p, r["mu"], r["sd"], rn, quote(w["up"]), quote(w["dn"]))


async def settle(sess, s):
    E = s + WIN
    await asyncio.sleep(max(0, E + 2 - time.time()))
    w = S.win.get(s)
    try:
        if w:
            # 自算结果只用Chainlink价(不退回币安代理); 开盘/收盘15秒内都要有Chainlink价才有效
            cl_ok = cl_near(s) and cl_near(E)
            r = fair(E + 1, s, S.cl, 1.0, p0_fix=official_p0(s)) if cl_ok and S.cl.last() is not None else None
            calc = ("Up" if r["c"] >= r["p0"] else "Down") if r else None
            outcome, src = None, "calc"
            for it in range(40):
                outcome = await gamma_outcome(sess, s)
                if outcome:
                    src = "polymarket"
                    break
                if it in (10, 39):
                    log.info("gamma结果未就绪 %s (第%d次): %s", s, it, await gamma_debug(sess, s))
                await asyncio.sleep(8)
            if outcome is None:
                outcome = calc
                if outcome is None:
                    log.warning("round %s 跳过: 开盘或收盘15秒内没有有效Chainlink价, 且未取到官方结果", s)
            if outcome:
                r = r or {}
                tw_ok = S.tw.t and S.tw.t[-1] >= E - 10
                p0_tw = official_p0(s)
                c_tw = S.tw.at(E) if tw_ok else None
                # 每个场次都记录结果(含没做预判的), 供 analyze.py 回测
                append_csv(f"{DATA}/outcomes.csv", {
                    "start": s, "outcome": outcome, "label": src, "calc_outcome": calc,
                    "p0_calc": round(r.get("p0", 0), 2), "c_calc": round(r.get("c", 0), 2),
                    "p0_tw": round(p0_tw, 2) if p0_tw else None, "c_tw": round(c_tw, 2) if c_tw else None})
                if w.get("pred"):
                    row = {"start": s, "time_utc": time.strftime("%m-%d %H:%M", time.gmtime(s)),
                           "pred": w["pred"], "p_pred": round(w["p_pred"], 4), "conf": int(w["conf"]),
                           "outcome": outcome, "label": src, "hit": int(w["pred"] == outcome),
                           "calc_outcome": calc, "p0_calc": round(r.get("p0", 0), 2),
                           "c_calc": round(r.get("c", 0), 2),
                           "p0_tw": round(p0_tw, 2) if p0_tw else None, "c_tw": round(c_tw, 2) if c_tw else None,
                           "ref": w["ref_name"], "sigma": round(S.sigma, 3), **w["f"],
                           "up_bid0": w["q0"][0], "up_ask0": w["q0"][1], "dn_bid0": w["q0"][2], "dn_ask0": w["q0"][3]}
                    append_csv(f"{DATA}/rounds_v2.csv", row)
                    S.rounds.append(row)
                    n = len(S.rounds)
                    acc = sum(x["hit"] for x in S.rounds) / n
                    cf = [x["hit"] for x in S.rounds if x["conf"]]
                    log.info("round %s pred=%s(%.3f) outcome=%s[%s] %s | n=%d acc=%.1f%% | conf n=%d acc=%s",
                             s, w["pred"], w["p_pred"], outcome, src, "HIT" if row["hit"] else "MISS", n,
                             acc * 100, len(cf), f"{sum(cf) / len(cf) * 100:.1f}%" if cf else "-")
    except Exception:
        log.exception("settle")
    if w and w.get("up"):
        await pm_sub([w["up"], w["dn"]], False)
    S.win.pop(s, None)


async def ticker(sess):
    while True:
        try:
            now = time.time()
            s0 = int(now // WIN) * WIN
            for s in (s0, s0 + WIN):
                if s not in S.win:
                    S.win[s] = {"start": s, "up": None, "dn": None, "pred": None, "pred_done": False, "ticks": []}
                    asyncio.create_task(init_window(sess, s))
                    asyncio.create_task(settle(sess, s))
            nxt = s0 + WIN
            if nxt - now <= LEAD and not S.win[nxt]["pred_done"]:
                make_pred(nxt)
            ref, rn = get_ref()
            if int(now) % 5 == 0:
                sg = est_sigma_h(ref, now)
                if sg:
                    S.sigma = 0.8 * S.sigma + 0.2 * sg
            shift = nowcast_shift(now) if rn == "chainlink" else 0.0
            v = {"now": now, "s0": s0, "ref": rn, "spot": ref.last(), "bn": S.bn.last(), "okx": S.okx.last(),
                 "sigma": S.sigma, "cur": None}
            cur = S.win[s0]
            if ref.last() is not None:
                op0 = official_p0(s0)
                r = fair(now, s0, ref, S.sigma, p0_fix=op0, shift=shift)
                if r:
                    r["p_raw"] = r["p"]
                    r["p"] = calibrate(r["p"], now - s0)
                    ub, ua = quote(cur["up"])
                    db, da = quote(cur["dn"])
                    r.update(end=s0 + WIN, up_bid=ub, up_ask=ua, dn_bid=db, dn_ask=da,
                             edge_up=(r["p"] - ua) if ua is not None else None,
                             edge_dn=((1 - r["p"]) - da) if da is not None else None,
                             pred=cur.get("pred"), p_pred=cur.get("p_pred"),
                             p0_src="official" if op0 is not None else "calc")
                    v["cur"] = r
                    cur["ticks"].append([round(now - s0, 1), round(r["p"], 4), ub, ua, db, da])
                    if int(now) % 2 == 0:
                        ft = features(now)
                        append_csv(f"{DATA}/ticks.csv", {
                            "ts": round(now, 1), "start": s0, "tau": round(now - s0, 1), "ref": rn,
                            "spot": round(ref.last(), 2), "bn": S.bn.last(), "okx": S.okx.last(),
                            "twap": S.tw.last(), "p0": round(r["p0"], 2), "p0_src": r["p0_src"],
                            "c_est": round(r["c"], 2), "p_up": round(r["p"], 4), "p_raw": round(r["p_raw"], 4),
                            "sd": round(r["sd"], 2), "up_bid": ub, "up_ask": ua, "dn_bid": db, "dn_ask": da,
                            "flow": ft["flow"], "obi": ft["obi"], "r1": ft["r1"], "bn_cl_bps": ft["bn_cl_bps"],
                            "shift": round(shift, 2), "sigma": round(S.sigma, 3)})
            nw = S.win[nxt]
            v["nxt"] = {"start": nxt, "pred": nw.get("pred"), "p": nw.get("p_pred"), "conf": nw.get("conf")}
            S.view = v
        except Exception:
            log.exception("ticker")
        await asyncio.sleep(max(0.05, 1 - (time.time() % 1)))


async def cl_watchdog():
    """订阅成功但价格流静默(只回一包快照后不再推送)时, 切换订阅方式并强制重连"""
    while True:
        await asyncio.sleep(5)
        try:
            if cl_stalled():
                S.cl_nofilter = not S.cl_nofilter
                log.warning("Chainlink价格流超过20秒无新数据, 强制重连并切换订阅方式(不带过滤=%s)", S.cl_nofilter)
                S.conn_t = time.time()
                await S.pm_ws.close()
        except Exception:
            log.exception("watchdog")


async def status():
    while True:
        await asyncio.sleep(30)
        now = time.time()
        age = {k: (f"{now - v:.0f}s" if v else "NEVER") for k, v in S.last_msg.items()}
        log.info("feeds %s | poly-auth=%s cl_src=%s | cl=%s tw=%s bn=%s okx=%s sigma=%.2f",
                 age, S.auth, S.cl_src, S.cl.last(), S.tw.last(), S.bn.last(), S.okx.last(), S.sigma)


# ------------------------------------------------------------------ Web 看板
def _auth(req):
    return not DASH_TOKEN or req.query.get("token") == DASH_TOKEN


async def api_state(req):
    if not _auth(req):
        return web.Response(status=401, text="bad token")
    now = time.time()
    ref, _ = get_ref()
    chart = [[t, ref.at(t), S.bn.at(t), S.okx.at(t), S.tw.at(t)] for t in range(int(now) - 420, int(now), 2)]
    cur = S.win.get(int(now // WIN) * WIN)
    rounds = list(S.rounds)
    n = len(rounds)
    cf = [x for x in rounds if x["conf"]]
    stats = {"n": n, "acc": sum(x["hit"] for x in rounds) / n if n else None,
             "cn": len(cf), "cacc": sum(x["hit"] for x in cf) / len(cf) if cf else None}
    feeds = {k: (now - v if v else None) for k, v in S.last_msg.items()}
    return web.json_response({**S.view, "chart": chart, "ticks": cur["ticks"] if cur else [],
                              "rounds": rounds[-25:], "stats": stats, "feeds": feeds,
                              "auth": S.auth, "cl_src": S.cl_src})


async def index(req):
    if not _auth(req):
        return web.Response(status=401, text="bad token")
    return web.Response(text=HTML, content_type="text/html")


HTML = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC 5分钟涨跌盘 监控</title><style>
body{background:#0e1116;color:#d6dbe3;font:13px/1.6 -apple-system,"PingFang SC","Microsoft YaHei","Noto Sans CJK SC",ui-monospace,Menlo,Consolas,monospace;margin:12px}
h2{margin:0 0 8px;font-size:16px;color:#e6edf3}h3{margin:16px 0 6px;color:#8aa;font-size:13px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}
.card{background:#161b22;border:1px solid #232a34;border-radius:6px;padding:8px}.k{color:#7d8590;font-size:11px}.v{font-size:17px}
.up{color:#3fb950}.dn{color:#f85149}.warn{color:#d29922}.mut{color:#7d8590}
table{border-collapse:collapse;width:100%}td,th{padding:3px 6px;border-bottom:1px solid #232a34;text-align:right;white-space:nowrap}th{color:#7d8590}
.wrap{overflow-x:auto}canvas{width:100%;height:230px;background:#161b22;border:1px solid #232a34;border-radius:6px}
.tip{color:#7d8590;font-size:12px;margin:2px 0 6px}</style></head><body>
<h2>BTC 5分钟涨跌盘 · 实时监控</h2>
<div id="feeds"></div>
<h3>当前场次</h3><div id="top" class="grid"></div>
<h3>价格走势（近7分钟）</h3><div class="tip">竖线 = 场次边界；绿色虚线 = 本场开盘基准价（收盘均价高于它则“涨”）</div><canvas id="c1"></canvas>
<h3>本场：模型上涨概率 与 盘口“涨”的买卖价</h3><div class="tip">横轴 = 开盘后秒数，240秒处为最后一分钟开始。绿线高于红线（涨卖一）越多，模型认为“涨”越被低估</div><canvas id="c2"></canvas>
<h3>下一场预判</h3><div id="nxt"></div>
<h3>历史场次</h3><div id="stats"></div><div class="wrap" id="tbl"></div>
<script>
const Q=location.search,$=id=>document.getElementById(id),f=(x,d=2)=>x==null?'-':Number(x).toFixed(d);
const ud=x=>({Up:'涨',Down:'跌'}[x]||x||'-');
const REF={'chainlink':'Chainlink','binance*':'币安(代理，未连上Chainlink)'};
const LAB={'calc':'自算','polymarket':'官方'};
const AUTH=a=>a=='ok'?'已认证':a=='no-creds'?'未配置凭据':a=='deriving'?'正在生成凭据':a=='derive-failed'?'凭据生成失败':a=='authenticating'?'认证中':(a||'').startsWith('FAILED')?'认证失败':a;
const card=(k,v,c='',tip='')=>`<div class="card" title="${tip}"><div class="k">${k}</div><div class="v ${c}">${v}</div></div>`;
const hm=s=>new Date(s*1000).toISOString().slice(11,16)+' UTC';
const age=a=>a==null?'<span class=dn>无数据</span>':(a<5?`<span class=up>${a.toFixed(1)}秒</span>`:`<span class=warn>${a.toFixed(0)}秒</span>`);
function plot(cv,lines,o={}){const dpr=devicePixelRatio||1,W=cv.clientWidth,H=cv.clientHeight;cv.width=W*dpr;cv.height=H*dpr;
const g=cv.getContext('2d');g.scale(dpr,dpr);let xs=[],ys=[];lines.forEach(l=>l.d.forEach(p=>{if(p[1]!=null){xs.push(p[0]);ys.push(p[1])}}));
(o.h||[]).forEach(h=>ys.push(h.y));if(!xs.length)return;
let x0=o.x0??Math.min(...xs),x1=o.x1??Math.max(...xs),y0=o.y0??Math.min(...ys),y1=o.y1??Math.max(...ys);if(y1-y0<1e-9){y1+=1;y0-=1}
if(o.y0==null){const p=(y1-y0)*.08;y0-=p;y1+=p}const X=x=>44+(x-x0)/(x1-x0||1)*(W-52),Y=y=>H-16-(y-y0)/(y1-y0)*(H-28);
g.font='10px monospace';g.lineWidth=1;for(let i=0;i<=4;i++){const y=y0+(y1-y0)*i/4;g.strokeStyle='#232a34';g.beginPath();g.moveTo(44,Y(y));g.lineTo(W-8,Y(y));g.stroke();g.fillStyle='#7d8590';g.fillText(y.toFixed(o.dec??1),2,Y(y)+3)}
(o.v||[]).forEach(v=>{if(v<x0||v>x1)return;g.strokeStyle='#555';g.beginPath();g.moveTo(X(v),10);g.lineTo(X(v),H-16);g.stroke()});
(o.h||[]).forEach(h=>{g.strokeStyle=h.c;g.setLineDash([4,3]);g.beginPath();g.moveTo(44,Y(h.y));g.lineTo(W-8,Y(h.y));g.stroke();g.setLineDash([])});
lines.forEach(l=>{g.strokeStyle=l.c;g.lineWidth=1.5;g.beginPath();let on=false;l.d.forEach(p=>{if(p[1]==null){on=false;return}const x=X(p[0]),y=Y(p[1]);if(on)g.lineTo(x,y);else{g.moveTo(x,y);on=true}});g.stroke()});
let lx=48;lines.forEach(l=>{g.fillStyle=l.c;g.font='11px sans-serif';g.fillText(l.n,lx,9);lx+=l.n.length*12+16})}
function draw(d){const c=d.cur,F=d.feeds||{};
$('feeds').innerHTML=`数据延迟：币安 ${age(F.bn)} ｜ 欧易 ${age(F.okx)} ｜ Chainlink现货 ${age(F.cl)} ｜ Chainlink 60秒均价 ${age(F.tw)} ｜ Polymarket盘口 ${age(F.pm)} ｜ 凭据状态：<b>${AUTH(d.auth)}</b> ｜ 参考价来源：<b>${REF[d.ref]||d.ref}</b>`;
let h=card('参考价',f(d.spot),'','用于结算判断的价格（Chainlink，未连上时用币安代理）')
+card('币安 / 欧易',f(d.bn,1)+' / '+f(d.okx,1),'','两家交易所的最新成交价')
+card('波动率 σ',f(d.sigma,2)+' 美元/√秒','','30秒尺度的价格波动，用于计算不确定度');
if(c){h+=card('本场剩余',Math.max(0,c.end-d.now).toFixed(0)+' 秒')
+card('开盘基准价',f(c.p0),'','开盘前60秒的Chainlink均价，开盘后固定不变。收盘均价≥它则判“涨”')
+card('预计收盘均价',f(c.c)+` (${c.mu>=0?'+':''}${f(c.mu,1)})`,c.mu>=0?'up':'dn','最后60秒的预计均价；括号内为与开盘基准价的差（美元）')
+card('收盘不确定度','± $'+f(c.sd,1),'','预计收盘均价的标准差，越接近收盘越小')
+card('模型：涨的概率',f(c.p,3),c.p>=.5?'up':'dn','模型估计的“涨”概率')
+card('“涨” 买一 / 卖一',f(c.up_bid,2)+' / '+f(c.up_ask,2),'','Polymarket上“涨”份额的最高买价 / 最低卖价')
+card('“跌” 买一 / 卖一',f(c.dn_bid,2)+' / '+f(c.dn_ask,2),'','Polymarket上“跌”份额的最高买价 / 最低卖价')
+card('模型优势：涨 / 跌',f(c.edge_up,3)+' / '+f(c.edge_dn,3),(c.edge_up>.05||c.edge_dn>.05)?'warn':'','模型概率 减 盘口卖价。为正说明该方向按模型被低估；超过0.05会高亮（未扣手续费）')
+card('本场预判',c.pred?`${ud(c.pred)} (${f(c.p_pred,3)})`:'无','','开盘前20秒做出的预判')}
$('top').innerHTML=h;
const bounds=[];for(let t=Math.ceil((d.now-420)/300)*300;t<=d.now;t+=300)bounds.push(t);
const L=(i,n,col)=>({n,c:col,d:d.chart.map(r=>[r[0],r[i]])});
plot($('c1'),[L(1,'参考价','#e6edf3'),L(2,'币安','#58a6ff'),L(3,'欧易','#a371f7'),L(4,'官方60秒均价','#f0883e')],{v:bounds,h:c?[{y:c.p0,c:'#3fb950'}]:[],dec:0});
const T=d.ticks||[];plot($('c2'),[{n:'模型：涨的概率',c:'#3fb950',d:T.map(r=>[r[0],r[1]])},{n:'“涨”卖一',c:'#f85149',d:T.map(r=>[r[0],r[3]])},{n:'“涨”买一',c:'#d29922',d:T.map(r=>[r[0],r[2]])}],{x0:0,x1:300,y0:0,y1:1,v:[240],h:[{y:.5,c:'#555'}],dec:2});
const n=d.nxt;$('nxt').innerHTML=n?`场次 ${hm(n.start)}：`+(n.pred?`<b class="${n.pred=='Up'?'up':'dn'}">${ud(n.pred)}</b>，概率 ${f(n.p,3)} ${n.conf?'（信号较强）':'（信号弱）'}`:`将在开盘前20秒给出预判（还有 ${Math.max(0,n.start-d.now).toFixed(0)} 秒）`):'';
const st=d.stats;$('stats').innerHTML=`共 ${st.n} 场，命中率 ${st.acc==null?'-':(st.acc*100).toFixed(1)+'%'} ｜ 信号较强 ${st.cn} 场，命中率 ${st.cacc==null?'-':(st.cacc*100).toFixed(1)+'%'}　<span class=mut>（开盘前预判长期接近50%属正常，样本需几百场才有意义）</span>`;
let t='<table><tr><th>场次(UTC)</th><th>预判</th><th>预判概率</th><th>结果</th><th>命中</th><th>结果来源</th><th>开盘基准(自算)</th><th>收盘均价(自算)</th><th>开盘基准(官方)</th><th>收盘均价(官方)</th><th>预判时卖价 涨/跌</th></tr>';
d.rounds.slice().reverse().forEach(r=>{t+=`<tr><td>${r.time_utc}</td><td>${ud(r.pred)}</td><td>${f(r.p_pred,3)}</td><td>${ud(r.outcome)}</td><td class="${r.hit?'up':'dn'}">${r.hit?'✔ 中':'✘ 错'}</td><td>${LAB[r.label]||r.label}</td><td>${f(r.p0_calc)}</td><td>${f(r.c_calc)}</td><td>${f(r.p0_tw)}</td><td>${f(r.c_tw)}</td><td>${f(r.up_ask0,2)} / ${f(r.dn_ask0,2)}</td></tr>`});
$('tbl').innerHTML=t+'</table>'}
async function tick(){try{const r=await fetch('/api/state'+Q);if(!r.ok){$('feeds').textContent=r.status==401?'访问口令错误：请在网址后加 ?token=你的口令（在服务器执行 btc5m url 查看）':'请求失败 HTTP '+r.status;return}draw(await r.json())}catch(e){$('feeds').textContent='无法连接服务器，稍后自动重试…'}}
tick();setInterval(tick,1000);
</script></body></html>'''


async def main(host, port):
    load_calib()
    async with aiohttp.ClientSession() as sess:
        app = web.Application()
        app.add_routes([web.get("/", index), web.get("/api/state", api_state)])
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, host, port).start()
        log.info("看板: http://%s:%s/%s", host, port, f"?token={DASH_TOKEN}" if DASH_TOKEN else "")
        await asyncio.gather(ws_poly(sess), ws_binance(sess), ws_okx(sess),
                             ticker(sess), pm_fallback(sess), cl_watchdog(), status())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9999)
    a = ap.parse_args()
    try:
        asyncio.run(main(a.host, a.port))
    except KeyboardInterrupt:
        pass
