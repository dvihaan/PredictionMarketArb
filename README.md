# Prediction market book recorder

Records public Kalshi and Polymarket order books for the rule-checked pairs in `watchlist.json`, then measures how often a hedged pair was profitable after fees, at what size, and for how long. It is read-only: it uses no API keys and places no orders.

## Use

Requires Python 3 and `requests`.

```bash
python test_edge.py
```

```bash
python record.py
```

```bash
python analyze.py
```

`record.py` runs until Ctrl+C and can be restarted at any time; it appends to `data/books.sqlite`. `analyze.py` can run while it records, and writes every episode to `data/episodes.csv`.

## Reading the results

- An episode's `end_reason` of `coverage_gap` or `missing_data` means its true length is unknown. `still_open` means it was still live when the data ended.
- Fills assume taker orders on both legs, with fees of `rate * p * (1 - p)` per contract and Kalshi fees rounded up to the cent per order. Check the current fee schedules before relying on them.
- Edges are measured from books fetched moments apart, so they may not have been fillable together. Gaps shorter than the polling interval (5 s by default) are invisible.
- A book row is only written when that book changes. Failed fetches are logged per cycle, so unchanged data and missing data stay distinct.

## Watchlist

`rule_status` records how each pair was checked:

- `read-close`: rules read on this pair or a same-event sibling, and judged close to equivalent.
- `template-read`: rules read on one pair in the family, with the same wording assumed for the rest.

Neither status clears a pair for trading. Read both venues' full rules first, because this list was built by comparing rule text on a few pairs, not all pairs.
