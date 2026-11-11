#!/usr/bin/env python3
"""
btc5m v2 —— Polymarket BTC 5分钟涨跌盘: 场次预判 + 场中公允概率 + 数据采集 + Web看板(9999)

结算规则(2026-08-14起): Chainlink BTC/USD 60秒TWAP
    Price to beat P0 = 开盘时刻 TWAP60 = 开盘前60秒均价
    结算价 C         = 收盘时刻 TWAP60 = 最后60秒均价
    C >= P0 => Up (平局算Up)
=> 只要知道已发生的价格, 就能算出 P(C>=P0) 的公允概率; 越接近收盘越确定。

环境变量:
    POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE  (Polymarket CLOB API凭据, 用于Chainlink价格流)
    DASH_TOKEN  (可选, 设置后访问看板需带 ?token=xxx)
服务器一行启动:
  curl -fsSL https://raw.githubusercontent.com/hrf100/btc5m/main/test4/run.sh | bash
Chainlink 凭据写在服务器 ~/.btc5m-4.env ，不要放进仓库。
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


S = St()


def get_ref():
    """结算用的参考价: Chainlink现货优先, 否则退回币安(代理)"""
    if S.cl_src == "chainlink" and S.cl.t and time.time() - S.last_msg["cl"] < 5:
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


def fair(t, s0, ref, sigma, p0_fix=None):
    """
    在时刻t, 估计 P(C >= P0)。
    P0 = 均值[s0-60, s0], C = 均值[s0+240, s0+300]。已发生部分用历史价, 未发生部分用当前价+布朗运动。
    sigma: 价格波动 $/sqrt(s)
    """
    e = s0 + WIN
    st = ref.at(t)
    if st is None:
        return None

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
    creds = _creds()
    gated = [{"channel": "price.crypto", "filter": {"symbol": "btcusd"}},
             {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}}]
    while True:
        try:
            async with sess.ws_connect(POLY_WS, heartbeat=None) as ws:
                S.pm_ws = ws
                if creds:
                    S.auth = "authenticating"
                    await ws.send_json({"op": "auth", "rid": "a1", "auth": {
                        "apiKey": creds[0], "secret": creds[1], "passphrase": creds[2]}})
                else:
                    log.warning("未设置 POLY_API_* 凭据: 没有Chainlink价格, 退回币安代理")
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
        if m.get("closed") and max(pr) >= 0.99 and min(pr) <= 0.01:
            return outs[pr.index(max(pr))]
    except Exception:
        pass
    return None


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
def append_csv(path, row):
    os.makedirs(DATA, exist_ok=True)
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
    r = fair(time.time(), s, ref, S.sigma)
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
            ref, rn = get_ref()
            r = fair(E + 1, s, ref, 1.0, p0_fix=official_p0(s)) if ref.last() is not None else None
            calc = ("Up" if r["c"] >= r["p0"] else "Down") if r else None
            outcome, src = None, "calc"
            for _ in range(40):
                outcome = await gamma_outcome(sess, s)
                if outcome:
                    src = "polymarket"
                    break
                await asyncio.sleep(8)
            if outcome is None:
                outcome = calc
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
            sg = est_sigma(S.bn, now)
            if sg:
                S.sigma = 0.9 * S.sigma + 0.1 * sg
            v = {"now": now, "s0": s0, "ref": rn, "spot": ref.last(), "bn": S.bn.last(), "okx": S.okx.last(),
                 "sigma": S.sigma, "cur": None}
            cur = S.win[s0]
            if ref.last() is not None:
                op0 = official_p0(s0)
                r = fair(now, s0, ref, S.sigma, p0_fix=op0)
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
                            "flow": ft["flow"], "obi": ft["obi"], "r1": ft["r1"], "bn_cl_bps": ft["bn_cl_bps"]})
            nw = S.win[nxt]
            v["nxt"] = {"start": nxt, "pred": nw.get("pred"), "p": nw.get("p_pred"), "conf": nw.get("conf")}
            S.view = v
        except Exception:
            log.exception("ticker")
        await asyncio.sleep(max(0.05, 1 - (time.time() % 1)))


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


HTML = r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC5m</title><style>
body{background:#0e1116;color:#d6dbe3;font:13px/1.5 ui-monospace,Menlo,Consolas,monospace;margin:12px}
h3{margin:16px 0 6px;color:#8aa}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}
.card{background:#161b22;border:1px solid #232a34;border-radius:6px;padding:8px}.k{color:#7d8590;font-size:11px}.v{font-size:17px}
.up{color:#3fb950}.dn{color:#f85149}.warn{color:#d29922}table{border-collapse:collapse;width:100%}
td,th{padding:3px 6px;border-bottom:1px solid #232a34;text-align:right}th{color:#7d8590}
canvas{width:100%;height:230px;background:#161b22;border:1px solid #232a34;border-radius:6px}</style></head><body>
<div id="feeds"></div><h3>当前场次</h3><div id="top" class="grid"></div>
<h3>价格 (近7分钟; 竖线=场次边界, 虚线=本场P0)</h3><canvas id="c1"></canvas>
<h3>本场: 公允P(Up) vs Polymarket Up买/卖 (横轴=开盘后秒数, 240处=最后一分钟开始)</h3><canvas id="c2"></canvas>
<h3>下一场预判</h3><div id="nxt"></div><h3>历史场次</h3><div id="stats"></div><div id="tbl"></div>
<script>
const Q=location.search,$=id=>document.getElementById(id),f=(x,d=2)=>x==null?'-':Number(x).toFixed(d);
const card=(k,v,c='')=>`<div class="card"><div class="k">${k}</div><div class="v ${c}">${v}</div></div>`;
const hm=s=>new Date(s*1000).toISOString().slice(11,16)+'Z';
function plot(cv,lines,o={}){const dpr=devicePixelRatio||1,W=cv.clientWidth,H=cv.clientHeight;cv.width=W*dpr;cv.height=H*dpr;
const g=cv.getContext('2d');g.scale(dpr,dpr);let xs=[],ys=[];lines.forEach(l=>l.d.forEach(p=>{if(p[1]!=null){xs.push(p[0]);ys.push(p[1])}}));
(o.h||[]).forEach(h=>ys.push(h.y));if(!xs.length)return;
let x0=o.x0??Math.min(...xs),x1=o.x1??Math.max(...xs),y0=o.y0??Math.min(...ys),y1=o.y1??Math.max(...ys);if(y1-y0<1e-9){y1+=1;y0-=1}
if(o.y0==null){const p=(y1-y0)*.08;y0-=p;y1+=p}const X=x=>44+(x-x0)/(x1-x0||1)*(W-52),Y=y=>H-16-(y-y0)/(y1-y0)*(H-28);
g.font='10px monospace';g.lineWidth=1;for(let i=0;i<=4;i++){const y=y0+(y1-y0)*i/4;g.strokeStyle='#232a34';g.beginPath();g.moveTo(44,Y(y));g.lineTo(W-8,Y(y));g.stroke();g.fillStyle='#7d8590';g.fillText(y.toFixed(o.dec??1),2,Y(y)+3)}
(o.v||[]).forEach(v=>{if(v<x0||v>x1)return;g.strokeStyle='#555';g.beginPath();g.moveTo(X(v),10);g.lineTo(X(v),H-16);g.stroke()});
(o.h||[]).forEach(h=>{g.strokeStyle=h.c;g.setLineDash([4,3]);g.beginPath();g.moveTo(44,Y(h.y));g.lineTo(W-8,Y(h.y));g.stroke();g.setLineDash([])});
lines.forEach(l=>{g.strokeStyle=l.c;g.lineWidth=1.5;g.beginPath();let on=false;l.d.forEach(p=>{if(p[1]==null){on=false;return}const x=X(p[0]),y=Y(p[1]);if(on)g.lineTo(x,y);else{g.moveTo(x,y);on=true}});g.stroke()});
let lx=48;lines.forEach(l=>{g.fillStyle=l.c;g.fillText(l.n,lx,9);lx+=l.n.length*7+14})}
const age=a=>a==null?'<span class=dn>无数据</span>':(a<5?`<span class=up>${a.toFixed(1)}s</span>`:`<span class=warn>${a.toFixed(0)}s</span>`);
function draw(d){const c=d.cur,F=d.feeds||{};
$('feeds').innerHTML=`数据延迟: 币安 ${age(F.bn)} | 欧易 ${age(F.okx)} | Chainlink现货 ${age(F.cl)} | Chainlink TWAP ${age(F.tw)} | Polymarket盘口 ${age(F.pm)} | PolyBolt认证: <b>${d.auth}</b> | 参考价: <b>${d.ref}</b>`;
let h=card('参考价',f(d.spot))+card('Binance / OKX',f(d.bn,1)+' / '+f(d.okx,1))+card('σ ($/√s)',f(d.sigma,2));
if(c){h+=card('本场剩余',Math.max(0,c.end-d.now).toFixed(0)+'s')+card('开盘TWAP (P0)',f(c.p0))
+card('预计收盘TWAP',f(c.c)+` (${c.mu>=0?'+':''}${f(c.mu,1)})`,c.mu>=0?'up':'dn')+card('收盘不确定度 sd','$'+f(c.sd,1))
+card('公允 P(Up)',f(c.p,3),c.p>=.5?'up':'dn')+card('Up 买/卖',f(c.up_bid,2)+' / '+f(c.up_ask,2))
+card('Down 买/卖',f(c.dn_bid,2)+' / '+f(c.dn_ask,2))
+card('Edge Up / Down',f(c.edge_up,3)+' / '+f(c.edge_dn,3),(c.edge_up>.05||c.edge_dn>.05)?'warn':'')
+card('本场预判',c.pred?`${c.pred} (${f(c.p_pred,3)})`:'无')}
$('top').innerHTML=h;
const s0=d.s0,bounds=[];for(let t=Math.ceil((d.now-420)/300)*300;t<=d.now;t+=300)bounds.push(t);
const L=(i,n,col)=>({n,c:col,d:d.chart.map(r=>[r[0],r[i]])});
plot($('c1'),[L(1,'参考价','#e6edf3'),L(2,'Binance','#58a6ff'),L(3,'OKX','#a371f7'),L(4,'CL-TWAP60','#f0883e')],{v:bounds,h:c?[{y:c.p0,c:'#3fb950'}]:[],dec:0});
const T=d.ticks||[];plot($('c2'),[{n:'公允P(Up)',c:'#3fb950',d:T.map(r=>[r[0],r[1]])},{n:'Up卖一',c:'#f85149',d:T.map(r=>[r[0],r[3]])},{n:'Up买一',c:'#d29922',d:T.map(r=>[r[0],r[2]])}],{x0:0,x1:300,y0:0,y1:1,v:[240],h:[{y:.5,c:'#555'}],dec:2});
const n=d.nxt;$('nxt').innerHTML=n?`场次 ${hm(n.start)}: `+(n.pred?`<b class="${n.pred=='Up'?'up':'dn'}">${n.pred}</b> p=${f(n.p,3)} ${n.conf?'(有信心)':'(弱信号)'}`:`开盘前${20}s给出预判 (还有 ${Math.max(0,n.start-d.now).toFixed(0)}s)`):'';
const st=d.stats;$('stats').innerHTML=`共 ${st.n} 场, 命中率 ${st.acc==null?'-':(st.acc*100).toFixed(1)+'%'} | 有信心 ${st.cn} 场, 命中率 ${st.cacc==null?'-':(st.cacc*100).toFixed(1)+'%'}`;
let t='<table><tr><th>场次</th><th>预判</th><th>p</th><th>结果</th><th>命中</th><th>结果来源</th><th>P0(算)</th><th>C(算)</th><th>P0(官方)</th><th>C(官方)</th><th>Up卖/Down卖@预判</th></tr>';
d.rounds.slice().reverse().forEach(r=>{t+=`<tr><td>${r.time_utc}</td><td>${r.pred}</td><td>${f(r.p_pred,3)}</td><td>${r.outcome}</td><td class="${r.hit?'up':'dn'}">${r.hit?'✔':'✘'}</td><td>${r.label}</td><td>${f(r.p0_calc)}</td><td>${f(r.c_calc)}</td><td>${f(r.p0_tw)}</td><td>${f(r.c_tw)}</td><td>${f(r.up_ask0,2)}/${f(r.dn_ask0,2)}</td></tr>`});
$('tbl').innerHTML=t+'</table>'}
async function tick(){try{const r=await fetch('/api/state'+Q);if(!r.ok){$('feeds').textContent='HTTP '+r.status;return}draw(await r.json())}catch(e){}}
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
                             ticker(sess), pm_fallback(sess), status())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9999)
    a = ap.parse_args()
    try:
        asyncio.run(main(a.host, a.port))
    except KeyboardInterrupt:
        pass
