# nordpool_collector

This repository contains regularly updated public market data scraped from
[NordPool](https://data.nordpoolgroup.com/). Collection started on 2024-09-09
and the job runs once per day via GitHub Actions
(`.github/workflows/collect.yml`).

The data covers all European bidding zones / areas served by NordPool
(UK excluded for simplicity) and is split into:

- **Day-ahead auction** — `data/auctions/day_ahead/` (prices and volumes).
  Hourly resolution up to 2025-09-30, then 15-minute resolution from 2025-10
  onward, following the Europe-wide switch to 15-minute market time units.
- **Intraday auctions 1, 2, 3** — `data/auctions/intraday_auction_{1,2,3}/`
  (prices and volumes), 15-minute resolution.
- **Intraday trading** — `data/intraday/<area>/`, hourly statistics
  (high, low, VWAP, open, close, VWAP1H, VWAP3H, buy/sell/transaction volumes,
  and first/last trade time in CET).

To make sure already-published values are captured even when NordPool revises
them (especially relevant for intraday trading), every run re-fetches and stores
the **last five days** (today plus the previous four). Consecutive daily files
therefore overlap heavily — deduplicate on the `date` column when combining them.

## File naming

Files are named `<collection-date>_<freq>.csv` (auctions) or
`<area>_<collection-date>_<freq>.csv` (intraday trading), where `<freq>` is the
pandas-inferred frequency of the contained series: `h` (hourly), `15min`, or
`None` when the frequency is irregular (DST-change days and the
hourly → 15-minute transition).

## Known caveats

- **Intraday trading gap:** due to a bug, `data/intraday/` was only populated
  once (2024-09-12). The bug has since been fixed, so collection resumes going
  forward — expect a gap between 2024-09-12 and the resumption date.
- **DST fall-back:** on the October clock-change day the repeated hour is stored
  as a duplicate `02:00:00` timestamp (the A/B distinction is not preserved).
  The March clock-change correctly omits the skipped `02:00` hour.
- Timestamps are local market time, normalized so each delivery day starts at
  `00:00`.

The data will be used for a personal project on electricity-market analysis and
forecasting.

## How it works

Pages are rendered and scraped with [selenium](https://pypi.org/project/selenium/)
and parsed with [beautifulsoup](https://pypi.org/project/beautifulsoup4/).

The code is inspired by
[this repo](https://github.com/uit-sok-1003-h24/notebooks/blob/9684fd1b29624e22be66705cbf249a148cfe30c4/res/scraping_nordpool.py)
and [this repo](https://github.com/elgohr/EPEX-DE-History).
