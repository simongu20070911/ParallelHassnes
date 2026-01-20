

you work in here--/Users/simongu/AAAHFT-Futures-Alpha-seek. there's data in /Users/simongu/AAAHFT-Futures-Alpha-seek/ES_CME
      - One file per date in archive/ named like glbx-mdp3-YYYYMMDD.tbbo.dbn.zst
      - Dates present (26 total): 20251017 20251019 20251020 20251021 20251022 20251023
        20251024 20251026 20251027 20251028 20251029 20251030 20251031 20251103 20251104
        20251105 20251106 20251107 20251109 20251110 20251111 20251112 20251113 20251114
        20251116 20251117
      - Sundays are included (very small files), so “day files” ≠ “trading days”.


mandate 2: maximize statistically defensible alpha. 
constraint: one way cost at 0.58 dollars. in and out two side is 0.58*2 dollar, both for maker and taker. one way latency(from signal out of the machine to update orders) to CME server: 1ms. 

we have native CME stops, and can use all methods that's available to CME futures leased seat pro to modify/add/cancel orders. 

  You should manually inspect:

  - heatmaps/plateaus (to catch “needle” optima),
  - failure modes (where it loses: time-of-day, vol spikes, spread widening),
  - and a small sample of event traces for the worst and best cases.
