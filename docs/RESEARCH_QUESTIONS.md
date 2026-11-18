# Research questions for discussion

## 1. Purpose

The recorder captures every order-book change and every trade for the matched pairs on Kalshi and
Polymarket. The next step is a research page in the browser, so anyone with the password can answer questions about the data without an AWS account.

This document lists the questions that page would answer first. Each question says what it asks, why it matters for the hypothesis, what one row of the answer means, which filters you could set, which data it uses, and its caveats.

## 2. The hypothesis

The project tests two claims:

1. **Gaps exist.** The same event is sometimes priced on the two venues so that buying YES on one and NO on
   the other costs less than $1 after fees. One of the two always pays $1, so the difference is a locked-in profit when the market resolves.
2. **They can be traded.** A real trader, reacting with a real delay, could buy at those prices in useful size, and the return beats leaving the money in 3-month Treasury bills.

There are two ways to trade a gap:

- **Taker on both legs:** buy both sides at once from the posted offers. The analysis calls each run of profitable time an *episode*.
- **Maker on one leg:** rest a bid on one venue and, when it fills, immediately buy the other side as a taker. The analysis simulates these as *maker fills*.

Published research on these two venues finds that gaps do appear, but most close within seconds to minutes or come from one venue's quote going stale while the other moves, and that gaps which never close are usually explained by differences in the two venues' rules. Thosese findings are to be tested on our collected data.

## 3. What we know so far 

- **Taker, Nov 16–17 (19.6 h of live books):** 538 episodes, which group into 379 opportunities. 13 are
  *actionable*: the rules were read on both venues, profit is at least $1 within a $1,000 budget, the return (including the interest the venues pay) beats the T-bill, and it was still profitable 1 s after it appeared. 142 more opportunities were excluded only because their pair's rules were read on a sibling market ("template-read"), not on the pair itself.
- **Maker, Nov 16–17 (28 h of live books), pairs with rules read on both venues, 1 s latency, cautious queue model:** 99 fills, $38.60 profit, 1.72¢ per contract, 23.3% a year on the capital committed until resolution. One pair (KXTIME-26-ZOH, Time Person of the Year) is 67% of that profit. Bids resting on Polymarket earn very little; almost everything comes from bids resting on Kalshi.

These are why several questions below look at reaction time, concentration in a few pairs, and rule status.

## 4. How the page would work

- **Guided mode:** pick a question from the list, set the filters, click Run. The answer is a table you can sort, filter and download as CSV (which opens in Excel).
- **SQL mode:** the SQL box the page already has, plus a library of the same questions as SQL, each with an explanation of what it does.
- **The two modes give the same answer.** Each guided question *is* a saved SQL query with blanks for the filters. "Show SQL" shows exactly what ran, and "Open in SQL mode" copies it into the SQL box to change. So the same choices always give the same result, and you can learn the SQL from the guided mode.
- **Runs in your browser:** free, nothing to install, no AWS account.
- **Charts come next:** a pair explorer (both venues' prices and the gap over time, with episodes and fills marked) is planned for the following stage.

## 5. The data behind the questions

All times are UTC. "Pair" means one Kalshi market matched to one Polymarket market.

| Table | One row is | What's in it |
|---|---|---|
| **pairs** | one pair, per day | both venues' questions, family (topic), rule status and notes, fees, expected resolution date |
| **pair quotes** (new) | each time either venue's best price or its size changes, for one pair | both venues' best prices and sizes; the gap after fees at those prices for both directions (Polymarket YES + Kalshi NO, and the reverse); how long since each venue's best price last changed |
| **episodes** | one taker episode | when, how long, profit and size walking the full order books, capital, return vs the T-bill, profit if the trader had reacted later, actionable or not |
| **maker fills** | one simulated maker fill and its hedge | pair, venue, price, size, edge, profit, the price move after the fill, model settings |
| **trades** | one trade on either venue | price in YES terms, size, which side was the taker |
| **coverage** | one recorder event | starts, stops, disconnects, resyncs: when the data has holes |
| **books** | one order-book change, top 25 levels | the raw record; large, for detailed SQL work |

The **gap after fees** is the profit per contract from buying the two sides at the best offers: $1 minus
both prices minus both venues' fees. Positive means a profit at the top of the books (depth is in episodes).

**pair quotes** is the one new table. It keeps only the moments when a best price or size changes, about a
tenth of the book updates. That keeps it small enough to query weeks of data in the browser.

## 6. The questions

### Q1. Gap overview per pair
- **Asks:** for each pair over the chosen dates, how much of the recorded time had a positive gap after fees, the largest gap, and the typical size available at the best price.
- **Why it matters:** the base rate for claim 1, and a ranking of which pairs deserve attention.
- **One row is:** one pair. **Filters:** dates, rule status, family, minimum gap (¢).
- **Data:** pair quotes, pairs.
- **Caveats:** best prices only; the size there can be tiny (Q3 walks the full books). Time when the recorder wasn't covering a market is left out, not counted as "no gap".

### Q2. Gap by minute for one pair
- **Asks:** for one pair, minute by minute, the largest gap after fees in each direction, the size at the best price, and how many times each venue's best price changed.
- **Why it matters:** shows *when* gaps open: news, time of day, or right after one venue moved. Patterns here suggest explanations to test. It is also the data the later chart will draw.
- **One row is:** one minute. **Filters:** pair, dates, direction.
- **Data:** pair quotes.

### Q3. Taker opportunities
- **Asks:** each opportunity: when it began, how long it lasted, the profit and size available at the start walking the full books, capital needed on each venue, the yearly return including interest versus the T-bill, and whether it is actionable.
- **Why it matters:** what a trader could actually have bought, not just the top price.
- **One row is:** one episode. **Filters:** dates, pair, rule status, minimum profit, capital budget ($1k / $10k / $50k), actionable only.
- **Data:** episodes.
- **Caveats:** Nov 9–12 were recorded by 5-second polling with long holes, so those days have no
  reaction-time or actionable answers.

### Q4. Survival by reaction time
- **Asks:** per pair, the share of opportunities still profitable if the trader acted 50 ms, 250 ms, 1 s or 5 s after the gap appeared, and the profit left at each delay.
- **Why it matters:** research says most gaps close within seconds to minutes. This tests it on our data and shows how fast a trader would need to be.
- **One row is:** one pair. **Filters:** dates, rule status, minimum profit.
- **Data:** episodes.
- **Caveats:** delays are measured from when our recorder saw the change. The recorder runs in Ohio, near Kalshi's servers; Polymarket's servers are in London. Placing real orders would add time.

### Q5. Stale quotes
- **Asks:** per pair, of the time a positive gap existed, the share during which one venue's best price
  hadn't changed for more than N seconds while the other venue's had.
- **Why it matters:** a gap against an old quote may be one nobody would still honor, or one venue simply
  not reacting yet. Research finds many apparent gaps look like this. It separates convincing gaps from
  questionable ones.
- **One row is:** one pair. **Filters:** dates, N seconds (for example 10, 60, 300), rule status.
- **Data:** pair quotes.
- **Caveats:** a quote that hasn't changed may still be good in a quiet market. The data can't tell whether it would really have filled; only placing an order would. (A per-episode stale flag was considered on  Nov 17 and not adopted, so this is measured per pair.)

### Q6. Which venue moves first
- **Asks:** per pair, when one venue's best price moves, how often the other venue's moves the same way within 1 s, 10 s and 60 s, and the typical delay. Reported both ways (Kalshi first, Polymarket first).
- **Why it matters:** shows where prices are set. A taker's gap opens when the leading venue moves and the slower one hasn't yet. A maker's bid resting on the slower venue gets hit by traders reacting to the leader's move, which is the "price moved against us after the fill" seen in the maker results.
- **One row is:** one pair and leading venue. **Filters:** dates, pair, time window.
- **Data:** pair quotes.
- **Caveats:** "the other venue followed" needs a definition we agree on (see the questions at the end).

### Q7. Maker results
- **Asks:** per pair or per resting venue: fills, contracts, quoted and realized edge per contract, profit, yearly return, and the price move 1 s, 10 s and 60 s after the fill, for each queue model and latency.
- **Why it matters:** tests the second way to trade, and its main risk: being filled just before the price  moves against you.
- **One row is:** one fill, or one pair in the summary version. **Filters:** dates, pair, rule status, resting venue, queue model, latency.
- **Data:** maker fills.
- **Caveats:** Polymarket fills lean low (its trade history averages each order's price, and trades on the NO side have no receive time). No rebates or liquidity rewards are counted.

### Q8. Market activity
- **Asks:** per pair, per day and venue: trades, contracts and dollar volume, the typical bid-ask spread, and the share of time both sides had quotes.
- **Why it matters:** how much money a pair could absorb. Maker fills come from other people's trades; a market that trades $50 a day can't fill $1,000 of bids.
- **One row is:** one pair, day and venue. **Filters:** dates, pair, venue.
- **Data:** trades, pair quotes.

### Q9. Data quality
- **Asks:** per day and venue: hours recorded, holes in coverage and recorder restarts, the share of book updates marked suspect, and the share of Polymarket trades without a receive time.
- **Why it matters:** decides which days and pairs to trust before drawing conclusions.
- **One row is:** one day and venue. **Filters:** dates, venue.
- **Data:** coverage, books, trades.
- **Caveats:** the suspect share reads the large books table; it may need a small daily summary to stay fast
  in the browser.

### Q10. Pairs reference
- **Asks:** the list of pairs with both questions, family, rule status and notes, fees, expected resolution date, and whether the pair is still recorded.
- **Why it matters:** gives the names used in every filter, and shows which pairs are template-read (rules read on a sibling market only, so the hedge may not hold).
- **One row is:** one pair. **Filters:** rule status, family, search by name.
- **Data:** pairs.

## 7. Additional points to explore
- **How gaps close:** which venue's move closes a gap?
- **Gaps by topic:** do politics, awards and rankings pairs behave differently? 
- **Gaps versus time to resolution:** do gaps shrink as resolution approaches? Needs weeks of data?
- **Fee change:** Polymarket announced lower fees for some categories from Nov 20. If that applies to the venue we record, compare before and after.
- **Around big trades:** what both books do in the seconds after a large trade on either venue?


