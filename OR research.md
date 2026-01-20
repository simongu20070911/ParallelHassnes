• It’s a posterior over your research beliefs: which hypotheses/policies work,
  in which regimes, with what robustness, and how confident you are.

  Concretely, maintain and update:

  - Belief objects: (hypothesis h, preset p, regime g)
  - Belief quantity: U(h,p,g) = expected net utility (net PnL after costs, with
    tail-risk/turnover penalties)
  - Posterior log contents (each run/day):
      - harness_id (data slice + latency/cost model + code hash)
      - h, p, g identifiers
      - evidence summary: n_trades, mean/median net pnl, drawdown/tail stats,
        trade-rate
      - posterior state: P(U>0), credible interval for U, effective sample size
        (with forgetting)
      - decision: promote/allocate credit/deprecate, with reason codes

  That is what your research system “gets updated on.”


› and over many intiial hypothesis?


• Maintain a posterior over a set of hypotheses by treating each hypothesis (and
  its presets) as an “arm” with uncertain utility.

  Structure:

  - Hypotheses h ∈ {h1…hK}
  - Each hypothesis has presets p ∈ P_h (plateau representatives)
  - Regimes g ∈ {g1…gM}

  Log and update posteriors for U(h,p,g) plus an aggregated view:

  - U(h,g) = max_p U(h,p,g) (best preset in that regime) or avg_p if you want
    stability
  - P(h is deployable in g) = P(U(h,g) > 0) with an uncertainty threshold

  Selection:

  - Allocate research effort via posterior uncertainty (explore high-
    uncertainty, high-upside hypotheses)
  - Allocate “credit” via posterior mean/quantile (exploit high-confidence
    winners)

  This is a portfolio of hypotheses with Bayesian updating and OR-style resource
  allocation.
