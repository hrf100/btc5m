#!/usr/bin/env python3
"""
analyze.py —— 用 btc5m.py 采集的数据回答: 优势到底在哪?
    python3 analyze.py [--fee 0.0] [--pmin 0.05] [--pmax 0.95] [--write-calib]
输入: data/ticks.csv  data/outcomes.csv  data/rounds_v2.csv
"""
import csv, json, math, argparse, os, collections

D = "data"


def fx(v):
    try:
        return None if v in ("", "None", None) else float(v)
    except Exception:
        return None


def load(name):
    p = f"{D}/{name}"
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def wilson(k, n, z=1.96):
    if n == 0:
        return (0, 0)
    ph = k / n
    d = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / d
    h = z * math.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def lg(p):
    p = min(max(p, 1e-4), 1 - 1e-4)
    return math.log(p / (1 - p))


def sig(z):
    return 1 / (1 + math.exp(-max(-30, min(30, z))))


def brier(ps, ys):
    return sum((p - y) ** 2 for p, y in zip(ps, ys)) / len(ps) if ps else float("nan")


def inv(M):
    n = len(M)
    A = [r[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, r in enumerate(M)]
    for c in range(n):
        pv = max(range(c, n), key=lambda r: abs(A[r][c]))
        if abs(A[pv][c]) < 1e-12:
            return None
        A[c], A[pv] = A[pv], A[c]
        d = A[c][c]
        A[c] = [x / d for x in A[c]]
        for r in range(n):
            if r != c:
                f = A[r][c]
                A[r] = [x - f * y for x, y in zip(A[r], A[c])]
    return [r[n:] for r in A]


def fit_logit(X, y, l2=1e-3, it=30):
    k = len(X[0])
    w = [0.0] * k
    H = None
    for _ in range(it):
        g = [-l2 * w[j] for j in range(k)]
        H = [[l2 if i == j else 0.0 for j in range(k)] for i in range(k)]
        for xi, yi in zip(X, y):
            p = sig(sum(a * b for a, b in zip(w, xi)))
            for j in range(k):
                g[j] += (yi - p) * xi[j]
                for l in range(k):
                    H[j][l] += p * (1 - p) * xi[j] * xi[l]
        Hi = inv(H)
        if Hi is None:
            break
        step = [sum(Hi[j][l] * g[l] for l in range(k)) for j in range(k)]
        w = [a + b for a, b in zip(w, step)]
        if max(abs(s) for s in step) < 1e-7:
            break
    Hi = inv(H) if H else None
    se = [math.sqrt(max(Hi[j][j], 0)) if Hi else float("nan") for j in range(k)]
    return w, se


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fee", type=float, default=0.0, help="手续费率(占买入成本的比例), 请按Polymarket实际规则填写")
    ap.add_argument("--pmin", type=float, default=0.05)
    ap.add_argument("--pmax", type=float, default=0.95)
    ap.add_argument("--write-calib", action="store_true")
    a = ap.parse_args()

    ticks, outs, rounds = load("ticks.csv"), load("outcomes.csv"), load("rounds_v2.csv")
    up = {int(r["start"]): (1 if r["outcome"] == "Up" else 0) for r in outs if r.get("outcome")}
    print(f"数据: ticks={len(ticks)}  有结果的场次={len(up)}  有预判的场次={len(rounds)}\n")

    # ---- 1. 开盘前预判: 53% 是否显著 -----------------------------------------
    print("=== 1. 开盘前预判 ===")
    if rounds:
        n = len(rounds)
        k = sum(int(r["hit"]) for r in rounds)
        lo, hi = wilson(k, n)
        print(f"命中 {k}/{n} = {k / n:.1%}, 95%置信区间 [{lo:.1%}, {hi:.1%}]  "
              + ("-> 区间包含50%, 尚不能说明有优势" if lo <= 0.5 else "-> 显著高于50%"))
        ps = [fx(r["p_pred"]) for r in rounds]
        ys = [up.get(int(r["start"]), 1 if r["outcome"] == "Up" else 0) for r in rounds]
        print(f"Brier={brier(ps, ys):.4f} (抛硬币=0.2500, 越低越好)")
        for lo_, hi_ in ((0, .02), (.02, .05), (.05, 1)):
            sub = [r for r in rounds if lo_ <= abs(fx(r["p_pred"]) - .5) < hi_]
            if sub:
                kk = sum(int(r["hit"]) for r in sub)
                print(f"  |p-0.5| in [{lo_:.2f},{hi_:.2f}): n={len(sub):4d} 命中 {kk / len(sub):.1%}")
        lab = collections.Counter(r["label"] for r in rounds)
        agree = [r["calc_outcome"] == r["outcome"] for r in rounds if r["label"] == "polymarket" and r["calc_outcome"]]
        print(f"结果来源: {dict(lab)};  自算结果与官方一致率: "
              + (f"{sum(agree) / len(agree):.1%} (n={len(agree)})" if agree else "-"))
        dif = [abs(fx(r["p0_calc"]) - fx(r["p0_tw"])) for r in rounds if fx(r["p0_tw"]) and fx(r["p0_calc"])]
        print("自算P0 与官方TWAP 平均偏差: " + (f"${sum(dif) / len(dif):.2f} (n={len(dif)})" if dif else "无官方TWAP数据(没配凭据?)"))
        refs = collections.Counter(r["ref"] for r in rounds)
        print(f"参考价来源: {dict(refs)}  (binance* 表示Chainlink没连上, 用币安代理, 精度会下降)")
        # 附加特征是否有信息
        if n >= 60:
            X = [[1.0, lg(fx(r["p_pred"])), fx(r["flow"]) or 0, fx(r["obi"]) or 0,
                  (fx(r["r1"]) or 0) / 10, (fx(r["bn_cl_bps"]) or 0) / 10] for r in rounds]
            w, se = fit_logit(X, ys)
            names = ["常数", "logit(模型p)", "主动买卖flow", "盘口失衡obi", "r1/10", "币安-CL价差/10"]
            print("逻辑回归 (结果~特征), z>2 才算有信息:" + ("  [样本<300, 仅供参考]" if n < 300 else ""))
            for nm, ww, ss in zip(names, w, se):
                print(f"  {nm:14s} 系数 {ww:+.3f}  z={ww / ss if ss else float('nan'):+.2f}")
    else:
        print("还没有预判数据")

    # ---- 2. 各时间点的校准 ---------------------------------------------------
    print("\n=== 2. 场中公允概率校准 (每10秒取样) ===")
    by = collections.defaultdict(list)   # (start) -> [(tau,p,up_ask,dn_ask)]
    for r in ticks:
        st, tau, p = int(r["start"]), fx(r["tau"]), fx(r["p_up"])
        if st in up and tau is not None and p is not None:
            by[st].append((tau, p, fx(r["up_ask"]), fx(r["dn_ask"])))
    BK = [(0, 60), (60, 120), (120, 180), (180, 240), (240, 270), (270, 300)]
    samples = {b: [] for b in BK}
    for st, rows in by.items():
        for b in BK:
            last_t = -99
            for tau, p, _, _ in sorted(rows):
                if b[0] <= tau < b[1] and tau - last_t >= 10:
                    samples[b].append((p, up[st]))
                    last_t = tau
    for b in BK:
        S_ = samples[b]
        if len(S_) < 30:
            print(f"tau {b[0]:3d}-{b[1]:3d}s: 样本不足({len(S_)})")
            continue
        ps, ys = [p for p, _ in S_], [y for _, y in S_]
        acc = sum((p >= .5) == (y == 1) for p, y in S_) / len(S_)
        print(f"tau {b[0]:3d}-{b[1]:3d}s: n={len(S_):5d} 方向命中 {acc:.1%}  Brier {brier(ps, ys):.4f}")
        edges = [0, .1, .3, .5, .7, .9, 1.0001]
        line = []
        for lo_, hi_ in zip(edges, edges[1:]):
            q = [(p, y) for p, y in S_ if lo_ <= p < hi_]
            if len(q) >= 15:
                line.append(f"p≈{sum(p for p, _ in q) / len(q):.2f}→实际{sum(y for _, y in q) / len(q):.2f}(n={len(q)})")
        print("      " + "  ".join(line))

    # ---- 3. 回测: 什么时候买有正期望 ----------------------------------------
    print(f"\n=== 3. 回测 (每场最多买1次, 成交按信号后下一个tick的卖价, 手续费率={a.fee}) ===")
    print("成交价过滤: [%.2f, %.2f];  break-even费率 = 单份平均盈利/平均买价" % (a.pmin, a.pmax))
    print(f"{'tau区间':>10} {'阈值':>5} {'笔数':>5} {'胜率':>6} {'均价':>6} {'每份盈亏':>8} {'前半':>7} {'后半':>7} {'盈亏平衡费率':>10}")
    for lo_t, hi_t in ((30, 240), (150, 270), (210, 285), (240, 290)):
        for thr in (0.03, 0.05, 0.08, 0.12):
            pnl, wins, asks, starts = [], 0, [], []
            for st in sorted(by):
                rows = sorted(by[st])
                for i in range(len(rows) - 1):
                    tau, p, ua, da = rows[i]
                    if not (lo_t <= tau < hi_t):
                        continue
                    _, _, ua2, da2 = rows[i + 1]
                    side = None
                    if ua is not None and p - ua > thr and ua2 is not None and a.pmin <= ua2 <= a.pmax:
                        side, ask, win = "up", ua2, up[st] == 1
                    elif da is not None and (1 - p) - da > thr and da2 is not None and a.pmin <= da2 <= a.pmax:
                        side, ask, win = "dn", da2, up[st] == 0
                    if side:
                        pnl.append((1 - ask if win else -ask) - a.fee * ask)
                        wins += win
                        asks.append(ask)
                        break
            if len(pnl) >= 5:
                h = len(pnl) // 2
                m = lambda x: sum(x) / len(x) if x else float("nan")
                avg = m(pnl)
                be = (avg + a.fee * m(asks)) / m(asks)
                print(f"{lo_t:>4}-{hi_t:<5} {thr:5.2f} {len(pnl):5d} {wins / len(pnl):6.1%} {m(asks):6.2f} "
                      f"{avg:+8.3f} {m(pnl[:h]):+7.3f} {m(pnl[h:]):+7.3f} {be:10.1%}")
    print("读法: '每份盈亏'为正且'前半/后半'同号, 才可能是真优势; 笔数<100 的行不要相信。")

    # ---- 4. Platt 校准 -------------------------------------------------------
    print("\n=== 4. 概率校准建议 (Platt: p' = sigmoid(a*logit(p)+b), 用后30%做样本外检验) ===")
    buckets = []
    pools = {b: samples[b] for b in BK}
    pre = [(fx(r["p_pred"]), up.get(int(r["start"]), 1 if r["outcome"] == "Up" else 0)) for r in rounds]
    pools[(-60, 0)] = pre
    for b, S_ in pools.items():
        if len(S_) < 200:
            continue
        cut = int(len(S_) * 0.7)
        tr, te = S_[:cut], S_[cut:]
        w, _ = fit_logit([[lg(p), 1.0] for p, _ in tr], [y for _, y in tr])
        raw = brier([p for p, _ in te], [y for _, y in te])
        cal = brier([sig(w[0] * lg(p) + w[1]) for p, _ in te], [y for _, y in te])
        ok = cal < raw - 0.002   # 至少改善0.002才采用, 避免拟合噪声
        print(f"tau {b[0]:4d}-{b[1]:3d}s: a={w[0]:.2f} b={w[1]:+.2f}  样本外Brier {raw:.4f} -> {cal:.4f}  {'采用' if ok else '不采用'}")
        if ok:
            wf, _ = fit_logit([[lg(p), 1.0] for p, _ in S_], [y for _, y in S_])
            buckets.append([b[0], b[1], round(wf[0], 4), round(wf[1], 4)])
    if a.write_calib and buckets:
        json.dump({"buckets": buckets}, open(f"{D}/calib.json", "w"))
        print(f"已写入 {D}/calib.json, 重启 btc5m.py 后生效")
    elif not buckets:
        print("没有满足条件(样本≥200且样本外变好)的档位, 暂不校准")


if __name__ == "__main__":
    main()
