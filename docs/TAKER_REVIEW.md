# Taker analysis — critical review of the assumptions

> **Decisions (Binoy, 2026-11-17):** adopted A1, A2 (amended), A3, B1, B2, B3 (without the stale criterion),
> C1, D1, D2. Not adopted: B4. C2 stays a documented limitation.
> - **A2 amended** after checking the venues' dates: Kalshi's expected expiration is a cautious backstop
>   (Alaska: 2027-11-03) and Polymarket's endDate is sometimes already past (Ljubljana: 2026-11-16). So the
>   pairs file's `est_resolution` stays the main date, plus a worst-case figure from the venues' dates.
> - **A1:** Kalshi APY from a dated rate file (no API); all 57 Polymarket markets have
>   `holdingRewardsEnabled: false` today.
>
> **Original status:** for Binoy's decision, 2026-11-17. The taker analysis (`analysis/episodes.py`, ported from
> Vihaan's `analyze_stream.py`) reproduces his results exactly. That shows the port is faithful, not that the
> assumptions are right. Below, each assumption is listed with why it matters for the hypothesis and a proposed
> change. Nothing changes until it's approved.

**The hypothesis being tested:** equivalent Kalshi and Polymarket contracts sometimes cost less than $1 combined
after fees, with enough depth and for long enough to lock in a profit **worth the capital it ties up**.

## A. Capital and carry: is the profit worth it?

**A1. Interest the venues pay is ignored.** The excess return compares each episode's annualized return with
the T-bill rate (4.26%), as if the capital earned nothing while tied up.
- **Kalshi** pays **3.50% APY on cash and open positions** (variable; eligible US users with an average balance
  of $250 or more).
- **Polymarket** pays **holding rewards of ~3.25–4%** on a short, changing list of long-dated political markets.
- *Why it matters:* the long-dated pairs (French presidential, 2027) earn ~1–2% annualized from the arbitrage
  itself. With ~3.5% carry on the capital, the total can **beat** the T-bill instead of losing to it. This may
  flip the conclusion for exactly the pairs with the most size.
- *Proposal:* report **total return = arbitrage + carry**: the Kalshi APY on the Kalshi leg's capital, and
  Polymarket holding rewards where a market is eligible. Store both rates daily with the reference data. Compare
  the total with the T-bill. Eligibility per Polymarket market needs a reliable source; check before relying on it.

**A2. Resolution dates are typed in by hand.** Annualizing uses `est_resolution` from the pairs file (e.g.
2027-05-15 for every French-president pair).
- *Why it matters:* annualized return scales with 1 / days-to-resolution, so a wrong date distorts every return
  figure, most for near-dated markets. Actual payout can also be later than the event: Polymarket's UMA
  resolution and disputes, or Kalshi's settlement timing.
- *Proposal:* take each venue's own close/expiry date from the daily market snapshots. Annualize to the
  **later** of the two legs (capital is tied up until both pay), plus a configurable settlement allowance
  (e.g. 7 days).

**A3. Capital is treated as one pot.** In reality the Polymarket leg needs USDC on Polygon and the Kalshi leg
needs USD at Kalshi, funded separately. Moving money between them takes time and may cost fees.
- *Proposal:* show the capital per venue for each episode. Fund-transfer costs are one-off and depend on the
  method, so document them rather than model them.

## B. Executability: could anyone actually trade it?

**B1. Results are reported at the episode's peak.** `peak_profit` / `peak_size` are the best moment in the
episode. A trader acting on a signal gets the conditions **at the moment they act**, not the later peak.
- *Proposal:* make **at-start** profit, size and edge the headline numbers, with the peak secondary. The latency
  columns already measure "start + L".

**B2. Size assumes walking the whole displayed book.** The largest episodes walk ~87,000 pairs for $127 (0.3¢
edge on ~$80k of capital). Displayed size competes with other traders, and capital is finite.
- *Proposal:* also report profit at **fixed capital budgets** (e.g. $1k, $10k, $50k), so results compare across
  pairs and reflect what one trader could deploy.

**B3. Any positive profit counts.** An episode worth $0.01 counts the same as one worth $500, so episode counts
overstate opportunity.
- *Proposal:* add an **"actionable"** flag:
  - profit at the $1k budget ≥ a minimum (e.g. $1)
  - total return (A1) above the T-bill
  - still profitable after a realistic reaction time (e.g. 1 s)
  - not stale (B4)

  Summaries lead with actionable counts.

**B4. Stale quotes aren't flagged per episode.** Published studies find most apparent gaps are one venue's
quote going stale while the other moves. 3.F measures this per pair/day, but episodes don't carry it.
- *Proposal:* a per-episode `stale_leg` flag: one leg's book unchanged for more than N seconds (e.g. 60) at the
  start while the other moved.

## C. Risk: is it really locked in?

**C1. Rule equivalence is assumed.** "Guaranteed profit if both legs pay out as one set" holds only if both
venues resolve identically.
- The pairs file records `rule_status`: `read-close` (31 pairs) or `template-read` (26, rules read on one
  sibling only), plus notes such as "Polymarket resolves Other if no result by 31 Dec 2027" or "Kalshi eligible
  for accelerated resolution".
- *Proposal:* break every summary down by `rule_status`; make `template-read` pairs **excluded from "actionable"
  by default** until their rules are read; show `rule_note` in the report. (Stage 4's pair agent can re-check
  rules.)

**C2. Leg risk isn't modelled.** Both legs are assumed to fill at once. If one fills and the other moves first,
the position is unhedged.
- *Proposal:* no new model now. The latency columns (both legs at +L) bound it. Note the limitation in the report.

## D. Data artifacts

**D1. Recorder restarts split episodes.** Each restart (code updates, the resize) closes open episodes as
`coverage_gap`, so one long opportunity appears as several. Today's Marine Le Pen gap shows up 4 times.
- *Proposal:* keep the honest split in the data, but in summaries **group episodes across gaps shorter than N
  seconds** (e.g. 120) as one opportunity, and count it once.

**D2. Fee rates are copied by hand into the pairs file.** Polymarket's market records now state the schedule
(`feeSchedule.rate` 0.04/0.05, `rebateRate` 0.25), and they match today. Kalshi's fee multiplier is published
per series.
- *Proposal:* keep "per-pair rates from the pairs file" (Binoy's decision), but have the nightly job **compare**
  them with the venues' published fees and flag any mismatch in the report.

## Not changing (noted)

- The fee formula `rate × p × (1 − p)` and Kalshi's per-order round-up match the published schedules.
- The 5-share Polymarket minimum per order is a real exchange rule; kept.
- Taxes, platform and counterparty risk: out of scope, mentioned in the report.
