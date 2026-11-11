#!/usr/bin/env python3
"""
Polymarket BTC 5分钟涨跌盘 —— 场次预判 + 数据采集 (阶段1)

数据源:
  - Chainlink BTC/USD : Polymarket RTDS websocket (crypto_prices_chainlink, 无需key)
  - Binance           : aggTrade + depth10  (现货 BTCUSDT)
  - OKX               : trades + books5     (BTC-USDT)
  - Polymarket        : Gamma(找token) + CLOB /book(买一卖一)

流程:
  每个5分钟场次开始前 LEAD 秒 -> 计算特征 -> 给出 Up/Down 预判 -> 记录盘口
  场次结束后 -> 用Chainlink起止价判定结果 -> 写入 CSV, 并打印滚动命中率
  场中每隔60s记录一次 Polymarket 买卖价 (为阶段2"低价入场"研究积累数据)

服务器一行启动:
  curl -fsSL https://raw.githubusercontent.com/hrf100/btc5m/main/run.sh | bash
"""
import asyncio, json, time, math, csv, os, logging
from collections import deque
import aiohttp

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
RTDS = "wss://ws-live-data.polymarket.com"
BN_WS = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth10@100ms"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"

WIN = 300            # 场次长度(秒)
LEAD = 20            # 提前多少秒预判
CONF_TH = 0.04       # |p_up-0.5| 超过此值才算"有信心"的信号
SAMPLE_OFFSETS = [60, 120, 180, 240]
OUT = "data/rounds.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("btc5m")

FEAT_NAME = {
    "r1": "1分钟", "r3": "3分钟", "r5": "5分钟", "r15": "15分钟",
    "flow": "主动买卖", "obi": "盘口", "lead": "币安领先", "bn_okx_bps": "币安减OKX",
}
SIDE = {"Up": "涨", "Down": "跌"}


def fmt_round(ts):
    return time.strftime("%m-%d %H:%M", time.localtime(ts))


def fmt_px(v):
    return "—" if v is None else f"{v:,.1f}"


def fmt_quote(bid, ask):
    one = lambda x: "—" if x is None else f"{x:.2f}"
    return f"{one(bid)}/{one(ask)}"


def fmt_feats(f):
    parts = []
    for k, name in FEAT_NAME.items():
        v = f.get(k, 0.0)
        parts.append(f"{name}{v:+.2f}" if k in ("flow", "obi") else f"{name}{v:+.1f}bp")
    return " ".join(parts)


class State:
    def __init__(self):
        self.cl_h = deque()   # (ts, px) Chainlink
        self.bn_h = deque()   # Binance 约1秒一个点
        self.okx_h = deque()
        self.flow = {"bn": deque(), "okx": deque()}   # (ts, signed_qty)
        self.obi = {"bn": 0.0, "okx": 0.0}
        self.last_msg = {"cl": 0, "bn": 0, "okx": 0}
        self.hits = []        # (hit, confident)


S = State()


def push(h, ts, px, min_gap=0.0, keep=1300):
    if h and ts - h[-1][0] < min_gap:
        h[-1] = (h[-1][0], px)
    else:
        h.append((ts, px))
    while h and h[0][0] < ts - keep:
        h.popleft()


def px_at(h, t):
    for ts, px in reversed(h):
        if ts <= t:
            return px
    return None


def last(h):
    return h[-1][1] if h else None


def flow_ratio(dq, now, sec=60):
    while dq and dq[0][0] < now - 180:
        dq.popleft()
    xs = [q for ts, q in dq if ts >= now - sec]
    tot = sum(abs(x) for x in xs)
    return sum(xs) / tot if tot > 0 else 0.0


# ---------------------------------------------------------------- websockets
async def pinger(ws, msg, sec):
    while True:
        await asyncio.sleep(sec)
        await ws.send_str(msg)


async def ws_chainlink(sess):
    sub = {"action": "subscribe", "subscriptions": [
        {"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": "btc/usd"})}]}
    while True:
        try:
            async with sess.ws_connect(RTDS) as ws:
                await ws.send_json(sub)
                pt = asyncio.create_task(pinger(ws, "PING", 5))
                try:
                    async for m in ws:
                        if m.type != aiohttp.WSMsgType.TEXT or not m.data or m.data == "PONG":
                            continue
                        try:
                            d = json.loads(m.data)
                        except Exception:
                            continue
                        pl = d.get("payload") or {}
                        if "data" in pl and isinstance(pl["data"], list):   # 初始快照
                            items = pl["data"]
                        elif "value" in pl:
                            items = [pl]
                        else:
                            continue
                        for it in items:
                            ts = it.get("timestamp", time.time() * 1000) / 1000
                            push(S.cl_h, ts, float(it["value"]))
                        S.last_msg["cl"] = time.time()
                finally:
                    pt.cancel()
        except Exception as e:
            log.warning("Chainlink 断开: %s", e)
        await asyncio.sleep(3)


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
                        # m=True: 买方是maker => 主动卖
                        S.flow["bn"].append((now, -q if d["m"] else q))
                        push(S.bn_h, now, px, min_gap=1.0)
                    elif "bids" in d:
                        b = sum(float(x[1]) for x in d["bids"])
                        a = sum(float(x[1]) for x in d["asks"])
                        S.obi["bn"] = (b - a) / (b + a) if b + a else 0.0
                    S.last_msg["bn"] = now
        except Exception as e:
            log.warning("币安断开: %s", e)
        await asyncio.sleep(3)


async def ws_okx(sess):
    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"},
                                       {"channel": "books5", "instId": "BTC-USDT"}]}
    while True:
        try:
            async with sess.ws_connect(OKX_WS) as ws:
                await ws.send_json(sub)
                pt = asyncio.create_task(pinger(ws, "ping", 20))
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
                                push(S.okx_h, now, px, min_gap=1.0)
                            elif ch == "books5":
                                b = sum(float(x[1]) for x in it["bids"])
                                a = sum(float(x[1]) for x in it["asks"])
                                S.obi["okx"] = (b - a) / (b + a) if b + a else 0.0
                        S.last_msg["okx"] = now
                finally:
                    pt.cancel()
        except Exception as e:
            log.warning("OKX 断开: %s", e)
        await asyncio.sleep(3)


# ---------------------------------------------------------------- 预判模型
# 权重是"先验", 不是拟合结果。跑出数据后用 CSV 里的特征做逻辑回归替换。
W = {"r1": 0.8, "r3": 0.6, "r5": 0.4, "r15": 0.2, "flow": 1.0, "obi": 0.5, "lead": 0.5}
SCALE = {"r1": 5, "r3": 10, "r5": 15, "r15": 30, "lead": 2}   # bps


def features(t):
    f = {}
    bn = last(S.bn_h)
    for k, sec in (("r1", 60), ("r3", 180), ("r5", 300), ("r15", 900)):
        p0 = px_at(S.bn_h, t - sec)
        f[k] = (bn / p0 - 1) * 1e4 if bn and p0 else 0.0
    f["flow"] = (flow_ratio(S.flow["bn"], t) + flow_ratio(S.flow["okx"], t)) / 2
    f["obi"] = (S.obi["bn"] + S.obi["okx"]) / 2
    cl = last(S.cl_h)
    # Binance 领先 Chainlink 的价差(bps): 正 => Chainlink 倾向向上追
    f["lead"] = (bn / cl - 1) * 1e4 if bn and cl else 0.0
    okx = last(S.okx_h)
    f["bn_okx_bps"] = (bn / okx - 1) * 1e4 if bn and okx else 0.0
    return f


def predict(f):
    s = 0.0
    for k, w in W.items():
        z = f[k] / SCALE[k] if k in SCALE else f[k]
        s += w * math.tanh(z)
    return 1 / (1 + math.exp(-0.6 * s))


# ---------------------------------------------------------------- Polymarket
async def get_tokens(sess, start):
    try:
        async with sess.get(f"{GAMMA}/markets", params={"slug": f"btc-updown-5m-{start}"},
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
            m = (await r.json())[0]
        ids, outs = m["clobTokenIds"], m["outcomes"]
        ids = json.loads(ids) if isinstance(ids, str) else ids
        outs = json.loads(outs) if isinstance(outs, str) else outs
        d = {o.lower(): i for o, i in zip(outs, ids)}
        return d.get("up"), d.get("down")
    except Exception as e:
        log.warning("Polymarket 市场查询失败: %s", e)
        return None, None


async def best(sess, token):
    if not token:
        return None, None
    try:
        async with sess.get(f"{CLOB}/book", params={"token_id": token},
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
            j = await r.json()
        bids = [float(x["price"]) for x in j.get("bids", [])]
        asks = [float(x["price"]) for x in j.get("asks", [])]
        return (max(bids) if bids else None, min(asks) if asks else None)
    except Exception:
        return None, None


# ---------------------------------------------------------------- 场次生命周期
async def settle(sess, start, f, p_up, up_tok, dn_tok, book0):
    samples = []
    bn_start = None
    for off in SAMPLE_OFFSETS:
        await asyncio.sleep(max(0, start + off - time.time()))
        if bn_start is None:
            bn_start = px_at(S.bn_h, start)
        ub, ua = await best(sess, up_tok)
        db, da = await best(sess, dn_tok)
        bn = last(S.bn_h)
        ret = (bn / bn_start - 1) * 1e4 if bn and bn_start else None
        samples.append({"off": off, "up_bid": ub, "up_ask": ua, "dn_bid": db, "dn_ask": da,
                        "bn_ret_bps": None if ret is None else round(ret, 2)})
    await asyncio.sleep(max(0, start + WIN + 3 - time.time()))
    c0, c1 = px_at(S.cl_h, start), px_at(S.cl_h, start + WIN)
    if c0 is None or c1 is None:
        log.warning("场次 %s 缺少 Chainlink 价格，本场跳过", fmt_round(start))
        return
    outcome = "Up" if c1 >= c0 else "Down"
    pred = "Up" if p_up >= 0.5 else "Down"
    conf = abs(p_up - 0.5) >= CONF_TH
    hit = pred == outcome
    S.hits.append((hit, conf))
    row = {"start": start, "pred": pred, "p_up": round(p_up, 4), "confident": int(conf),
           "outcome": outcome, "hit": int(hit), "cl_start": c0, "cl_end": c1,
           **{k: round(v, 4) for k, v in f.items()},
           "up_bid0": book0[0], "up_ask0": book0[1], "dn_bid0": book0[2], "dn_ask0": book0[3],
           "samples": json.dumps(samples)}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    new = not os.path.exists(OUT)
    with open(OUT, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)
    n = len(S.hits)
    acc = sum(h for h, _ in S.hits) / n
    cf = [h for h, c in S.hits if c]
    log.info("场次 %s 预判%s(上涨%.1f%%) 结果%s %s | 全部%d场 命中率%.1f%% | 有信心%d场 命中率%s",
             fmt_round(start), SIDE[pred], p_up * 100, SIDE[outcome], "命中" if hit else "未中",
             n, acc * 100, len(cf), f"{sum(cf) / len(cf) * 100:.1f}%" if cf else "—")


async def window_loop(sess):
    while True:
        now = time.time()
        start = (int(now) // WIN + 1) * WIN
        await asyncio.sleep(max(0, start - LEAD - now))
        t = time.time()
        f = features(t)
        p_up = predict(f)
        up_tok, dn_tok = await get_tokens(sess, start)
        (ub, ua), (db, da) = await asyncio.gather(best(sess, up_tok), best(sess, dn_tok))
        side = "涨" if p_up >= 0.5 else "跌"
        log.info("下场 %s 预判%s 上涨概率%.1f%% | %s | 涨盘买/卖 %s 跌盘买/卖 %s",
                 fmt_round(start), side, p_up * 100, fmt_feats(f), fmt_quote(ub, ua), fmt_quote(db, da))
        asyncio.create_task(settle(sess, start, f, p_up, up_tok, dn_tok, (ub, ua, db, da)))
        await asyncio.sleep(max(0, start + 1 - time.time()))


async def status():
    while True:
        await asyncio.sleep(30)
        now = time.time()
        names = {"cl": "Chainlink", "bn": "币安", "okx": "OKX"}
        age = " ".join(f"{names[k]}{'从未收到' if not v else f'{now - v:.0f}秒前'}" for k, v in S.last_msg.items())
        log.info("行情 %s | 价格 Chainlink %s 币安 %s OKX %s",
                 age, fmt_px(last(S.cl_h)), fmt_px(last(S.bn_h)), fmt_px(last(S.okx_h)))


async def main():
    log.info("采集已启动，每5分钟场次开始前 %d 秒预判，结果写入 %s", LEAD, OUT)
    async with aiohttp.ClientSession() as sess:
        await asyncio.gather(ws_chainlink(sess), ws_binance(sess), ws_okx(sess),
                             window_loop(sess), status())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
