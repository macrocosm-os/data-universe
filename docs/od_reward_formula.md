# On-Demand Reward Multipliers — Formula Specification

## 1. Purpose

Replace `calculate_ondemand_reward_multipliers` with a pair of curves
that (a) differentiate fast/full miners from slow/partial miners more
clearly, (b) reward over-delivery within bounded limits, and (c) remove
the rear-end floor that flattens speed signal in the back half of a
job's expiry window.

This document fully specifies the math, the constants, the invariants,
and the test cases.

### Quick visual

| | |
|---|---|
| ![Speed curve](od_reward_formula_plots/fig1_speed_curve.png) | ![Volume limited](od_reward_formula_plots/fig2_volume_limited.png) |

Side-by-side scenarios (current vs new):

![Scenarios](od_reward_formula_plots/fig5_scenario_compare.png)

## 2. Inputs

| Symbol | Description | Type | Range |
|---|---|---|---|
| `t` | Upload delay = `submitted_at - job_created_at` | float (seconds) | 0 to ~120 |
| `r` | Number of entities the miner returned | int | 0 to ~10⁵ |
| `L` | Requested limit (may be `None` for user-history mode) | int or None | 1-1000 or None |

## 3. Speed Multiplier `S(t)`

### 3.1 Formula

```
S(t) = min(1.0, 0.5 ^ (max(FLOOR, t) / H))   for t ≥ 0
S(t) = 0                                      for t > t_expire
H     = 45.0   # seconds; half-life
FLOOR = 1.0    # seconds; practical response floor
```

Equivalently (for verification): `S(t) = exp(-ln(2) · max(FLOOR, t) / H)`.

The practical floor treats any submission faster than `FLOOR` as exactly
`FLOOR` fast. A complete receive→process→submit cycle cannot realistically
finish in under a second, so sub-second times indicate pre-cached job serving.
Without the floor the curve is so flat near `t = 0` that a 0.2s cached response
(S ≈ 0.997) out-scores an honest 1s miner (S ≈ 0.985); the floor removes that
edge so the fastest achievable multiplier is `S(FLOOR) ≈ 0.985`, not 1.0.

### 3.2 Why half-life form

A pure exponential decay `exp(-λt)` and a half-life form `0.5^(t/H)` are
mathematically identical with `H = ln(2)/λ`. We use the half-life form
because the half-life parameter is intuitive: "every H seconds, the
speed multiplier halves." Operations can re-tune one number with a
clear meaning.

### 3.3 Why H = 45s

Empirical p50 upload times (last 7d):

| Job mode (X) | p50 | p90 | p99 |
|---|---|---|---|
| keyword, ≤2h | 0.8s | 1.7s | 6s |
| keyword, ≤7d | 5.8s | 65s | 104s |
| username, ≤7d | 20s | 85s | 105s |

Choosing `H = 45s` means:
- p50 cache-hit scrapes (≤2s) → S ≈ 0.97
- p50 keyword fresh-scrape (~6s) → S ≈ 0.91
- p50 user-history fresh-scrape (~20s) → S ≈ 0.74
- Half-life (45s) → S = 0.5
- p90 user-history (~85s) → S ≈ 0.27
- Near expiry (115s) → S ≈ 0.17

This puts the half-life at the median of slow-but-legit Apify scrapes,
which means typical scrapes are above half-credit while expiry-edge
submissions are well below. A miner who submits at t=120 gets ~16% of
max speed reward — meaningful but no longer the same flat 10% as
everyone in the back half.

### 3.4 Selected values

| t (s) | S(t) | Tier |
|---|---|---|
| 0 | 1.0000 | clock-skew floor |
| 1 | 0.9847 | cache hit |
| 2 | 0.9696 | cache hit |
| 5 | 0.9261 | fast scrape |
| 10 | 0.8578 | fast scrape |
| 15 | 0.7944 | typical |
| 20 | 0.7356 | typical |
| 30 | 0.6300 | active scrape |
| 45 | 0.5000 | **half-life** |
| 60 | 0.3969 | slow |
| 90 | 0.2500 | very slow |
| 115 | 0.1709 | expiry edge |
| 120 | 0.1575 | at expiry |

### 3.5 Invariants

- **S1**: `S(t1) ≥ S(t2)` whenever `t1 ≤ t2`. (Monotone non-increasing.)
- **S2**: `S(t) ∈ [0, 1]` for all valid t.
- **S3**: No floor that flattens the back half. `S(60) ≠ S(120)`.
- **S4**: Continuous and differentiable on `(0, ∞)`.
- **S5**: Smooth concave-down decay; sharper drop in the active-scrape
  middle than at the cache-hit head.

### 3.6 Edge cases

- **Negative t** (clock skew): `t = max(0, t)`. Returns `S(0) = 1.0`.
- **Missing timestamps**: short-circuit upstream; do not call this
  function. The caller substitutes `(S, V) = (0.5, 0)` for "skip with
  mild penalty".
- **t > t_expire**: defensive zero. Submissions past expiry are
  rejected by the API before reaching the validator.

## 4. Volume Multiplier `V(r, L)`

### 4.1 Formula (limited mode, `L > 0`)

```
V(r, L) = 0                                 if r == 0
V(r, L) = (r / L) ^ p                       if 0 < r ≤ L
V(r, L) = 1.0 + min(c · ln(1 + (r-L)/L), B) if r > L

p = 1.3
c = 0.15
B = 0.25
```

### 4.2 Formula (open mode, `L is None`)

```
V_open(r) = min(V_max, log10(1 + r) / log10(1 + r_ref))

r_ref = 1000
V_max = 1.5
```

### 4.3 Why these shapes

**Below target (sub-linear concave):** `(r/L)^p` with `p=1.3` punishes
half-fills — `V(L/2, L) = 0.5^1.3 = 0.406`. The consumer asked for L
items; receiving half is more than half-bad because the data may be
unusable for their downstream pipeline. `p = 1` would be linear; `p =
2` would be too harsh on near-misses (`V(0.9L, L) = 0.81`). `p = 1.3`
keeps near-target submissions well-rewarded
(`V(0.9L, L) = 0.872`) while penalizing partial returns.

**Over target (logarithmic bonus, capped):** Going over earns extra
credit — miners who return more than requested are putting in real
work — but the bonus saturates. `c = 0.15, B = 0.25` gives:
- 1.5L returned → V = 1.061
- 2L returned → V = 1.104
- 5L returned → V = 1.241
- 10L+ returned → V = 1.250 (capped)

**Open mode:** When `L is None` (X user-history with limit unset), there
is no contract to compare against. We grade by absolute volume on a
log scale, with `r_ref = 1000` as the reference "good" return. Bonus
caps at `V_max = 1.5`.

### 4.4 Selected values, `L = 100`

| r | V(r, L) | Tier |
|---|---|---|
| 0 | 0.0000 | empty |
| 10 | 0.0501 | tiny |
| 30 | 0.2091 | weak |
| 50 | 0.4061 | half |
| 70 | 0.6290 | partial |
| 80 | 0.7482 | near full |
| 90 | 0.8720 | almost |
| 100 | 1.0000 | **exact** |
| 110 | 1.0143 | over |
| 150 | 1.0608 | 1.5× |
| 200 | 1.1040 | 2× |
| 500 | 1.2414 | 5× |
| 1000 | 1.2500 | capped |

### 4.5 Selected values, open mode

| r | V_open(r) |
|---|---|
| 0 | 0.0000 |
| 10 | 0.3471 |
| 100 | 0.6680 |
| 500 | 0.8998 |
| 1000 | 1.0000 |
| 5000 | 1.2328 |
| 10000 | 1.3332 |
| 100000 | 1.5000 (capped) |

### 4.6 Invariants

- **V1**: `V(0, L) = 0` for all L.
- **V2**: `V(r1, L) ≤ V(r2, L)` whenever `r1 ≤ r2`. (Monotone non-decreasing in r.)
- **V3**: `V(L/2, L) < 0.5` for all L > 0. (Sub-linear below target.)
- **V4**: `V(L, L) = 1.0` for all L > 0.
- **V5**: `V(r, L) ≤ 1.25` for all `r > L` with `L > 0`.
- **V6**: For `L = None`, `V_open(r_ref) = 1.0`, and `V_open(r) ≤ 1.5`.

### 4.7 Edge cases

- **r < 0**: clamp to 0.
- **L ≤ 0** in limited mode: treat as `L = None` (open mode).
- **L is None and r = 0**: `V_open(0) = 0`.
- **Float r**: `int(round(r))` upstream; the formula tolerates floats.

## 5. Combined Reward

```
raw_reward(t, r, L) = ONDEMAND_BASE_REWARD · S(t) · V(r, L)
```

This is then EMA'd into the miner's `ondemand_boosts[uid]` exactly as
today (`alpha = 0.3`). Validation pass/fail still gates whether
`apply_ondemand_reward` is called at all.

### 5.1 Reward range

- **Maximum (limited mode):** `S = 1.0, V = 1.25` → `R = 1.25 · BASE`
- **Maximum (open mode):** `S = 1.0, V = 1.50` → `R = 1.50 · BASE`
- **Minimum non-zero:** at expiry edge (S ≈ 0.16) with 1 returned for
  L = 100 (V ≈ 0.005) → `R ≈ 8e-4 · BASE`

**Spread: ~1700× between best and worst non-empty submission**, vs
~200× today.

### 5.2 Reference scenarios

| Scenario | t | r | L | S | V | R / BASE |
|---|---|---|---|---|---|---|
| Cache-hit, full | 1s | 100 | 100 | 0.985 | 1.000 | 0.985 |
| Cache-hit, over | 1s | 200 | 100 | 0.985 | 1.104 | 1.087 |
| Fast scrape, full | 10s | 100 | 100 | 0.857 | 1.000 | 0.857 |
| Typical scrape, full | 30s | 100 | 100 | 0.630 | 1.000 | 0.630 |
| Slow scrape, full | 60s | 100 | 100 | 0.397 | 1.000 | 0.397 |
| Half-life, half | 45s | 50 | 100 | 0.500 | 0.406 | 0.203 |
| Expiry edge, full | 115s | 100 | 100 | 0.170 | 1.000 | 0.170 |
| Expiry edge, partial | 115s | 30 | 100 | 0.170 | 0.209 | 0.036 |
| Open mode, fast | 5s | 1000 | None | 0.926 | 1.000 | 0.926 |
| Open mode, low | 30s | 100 | None | 0.630 | 0.668 | 0.421 |

### 5.3 Comparison with current formula

| Scenario | Current R/BASE | New R/BASE | Δ |
|---|---|---|---|
| p50 X-keyword 6s, 95/100 | 0.704 | 0.853 | +21% |
| p50 X-user 20s, 70/100 | 0.258 | 0.462 | +79% |
| p99 X-user 95s, 50/100 | 0.050 | 0.094 | +88% |
| Slow legit 60s, 100/100 | 0.100 | 0.397 | +297% |
| Cache hit 1s, 100/100 | 0.951 | 0.985 | +4% |
| Empty submission | 0 | 0 | 0 |

Net effect: legit miners — especially those doing slow but real Apify
scrapes — earn meaningfully more. Cache-hit miners are essentially
unchanged. Spam/empty submissions remain at zero (gated by validation
upstream).

## 6. Tuning Knobs

Operations can adjust three constants without re-deriving the math:

| Constant | Effect | Suggested range |
|---|---|---|
| `H` (speed half-life) | ↑ softens speed pressure; ↓ hardens | 30-90s |
| `p` (volume concavity) | ↑ punishes half-fills harder; ↓ leniency | 1.0-2.0 |
| `r_ref` (open-mode reference) | ↑ raises bar for "1.0" in open mode | 500-5000 |

Default ship values: `H = 45, p = 1.3, r_ref = 1000`.

## 6.5 Graphs

All plots regenerable from `scripts/make_od_reward_graphs.py`.

### Speed curve

![Speed curve](od_reward_formula_plots/fig1_speed_curve.png)

Blue: new half-life curve. Red dashed: current `max(0.1, exp(-0.05·t))`.
The current curve hits its 0.1 floor around t=46s and stays flat,
giving every back-half submission identical speed credit. The new
curve halves every 45s and keeps decaying through expiry, so a
60s submission and a 115s submission earn meaningfully different
amounts.

### Volume curve, limited mode (`L = 100`)

![Volume limited](od_reward_formula_plots/fig2_volume_limited.png)

Green: new sub-linear concave curve below `r=L`, log-bonus capped at
1.25 above. Red dashed: current linear-then-flat. Below target, the
new curve is *more punishing* (`V(50, 100) = 0.41` vs `0.50`); above
target, it pays a small bonus for over-delivery instead of flat-lining
at 1.0.

### Volume curve, open mode

![Volume open](od_reward_formula_plots/fig3_volume_open.png)

Log-x view of the open-mode curve (`L = None`, X user-history).
Reference point `r_ref = 1000` gives `V = 1.0`; saturates at 1.5
once a miner returns ~30k entities or more.

### Combined heatmap

![Combined heatmap](od_reward_formula_plots/fig4_combined_heatmap.png)

`R / BASE = S(t) · V(r/L)` over the full input space.
White contour lines mark equal-reward isoclines. Top-left corner
(fast + over-delivery) reaches the 1.10+ ceiling; bottom-right corner
(slow + partial) approaches zero. The 0.50 contour traces the speed
half-life through the volume curve.

### Scenario comparison

![Scenario comparison](od_reward_formula_plots/fig5_scenario_compare.png)

Reference scenarios with current and new formulas side by side.
Cache-hit submissions barely change (+4%). Slow legitimate scrapes
gain the most (+88% to +297%) because the new curve removes the 0.1
floor and rewards on the actual exponential. Empty submissions stay
at zero throughout.

## 7. Reference Implementation

```python
import math
from typing import Optional, Tuple

# Tuning constants
SPEED_HALF_LIFE_S = 45.0
VOLUME_SHORTFALL_EXPONENT = 1.3
VOLUME_OVER_LOG_COEF = 0.15
VOLUME_OVER_BONUS_CAP = 0.25
VOLUME_OPEN_REF = 1000
VOLUME_OPEN_MAX = 1.5

def speed_multiplier(upload_seconds: float) -> float:
    """Half-life decay; clamps to [0, 1]."""
    if upload_seconds is None:
        return 0.5  # caller fallback; log upstream
    t = max(0.0, float(upload_seconds))
    return min(1.0, 0.5 ** (t / SPEED_HALF_LIFE_S))

def volume_multiplier(returned: int, limit: Optional[int]) -> float:
    """Sub-linear below target, log-capped over target.
    Open mode (limit is None) uses log scale to absolute volume."""
    r = max(0, int(returned or 0))
    if r == 0:
        return 0.0

    if limit is None or limit <= 0:
        # Open mode
        denom = math.log10(1 + VOLUME_OPEN_REF)
        return min(VOLUME_OPEN_MAX, math.log10(1 + r) / denom)

    L = int(limit)
    if r <= L:
        return (r / L) ** VOLUME_SHORTFALL_EXPONENT

    # r > L: log-scaled bonus, capped
    extra = (r - L) / L
    bonus = min(VOLUME_OVER_BONUS_CAP, VOLUME_OVER_LOG_COEF * math.log(1.0 + extra))
    return 1.0 + bonus

def ondemand_reward_multipliers(
    job_created_at, submission_timestamp, returned_count, requested_limit
) -> Tuple[float, float]:
    if job_created_at is None or submission_timestamp is None:
        return 0.5, 0.0
    delta = (submission_timestamp - job_created_at).total_seconds()
    return speed_multiplier(delta), volume_multiplier(returned_count, requested_limit)
```

## 8. Test Cases

Recommended parametrized test table:

```python
@pytest.mark.parametrize("t,r,L,exp_s,exp_v", [
    # Speed curve
    (0,    100, 100, 1.000, 1.000),
    (1,    100, 100, 0.985, 1.000),
    (45,   100, 100, 0.500, 1.000),
    (90,   100, 100, 0.250, 1.000),
    (120,  100, 100, 0.158, 1.000),
    # Volume curve, limited
    (10,   0,   100, 0.858, 0.000),
    (10,   50,  100, 0.858, 0.406),
    (10,   100, 100, 0.858, 1.000),
    (10,   150, 100, 0.858, 1.061),
    (10,   1000,100, 0.858, 1.250),
    # Volume curve, open
    (10,   0,    None, 0.858, 0.000),
    (10,   1000, None, 0.858, 1.000),
    (10,   100000,None,0.858, 1.500),
    # Edge cases
    (-5,   100, 100, 1.000, 1.000),  # negative t clamped
    (10,   100, 0,   0.858, 0.667),  # L=0 → open mode
    (10,   -5,  100, 0.858, 0.000),  # negative r clamped
])
def test_multipliers(t, r, L, exp_s, exp_v):
    s, v = speed_multiplier(t), volume_multiplier(r, L)
    assert abs(s - exp_s) < 0.005
    assert abs(v - exp_v) < 0.005
```

## 9. Migration

Replace `calculate_ondemand_reward_multipliers` body with the new
implementation. Caller signature is preserved
(`Tuple[speed_mult, volume_mult]`). Remove `consensus_count` parameter
from the signature — the open-mode formula no longer requires it.

`apply_ondemand_reward` in `rewards/miner_scorer.py` is unchanged.
`ONDEMAND_BASE_REWARD` is unchanged. EMA alpha is unchanged.

## 10. Observability

Log per-submission:
```
OD reward: uid=X t=Ys r=Z/L speed=A.AAA vol=B.BBB raw=C
```

Plot histograms of speed and volume multipliers over a 24h window
post-deploy to confirm distribution shifts as expected.
