-- ============================================================
-- historical_yield_trend — A01115
--
-- Per-year historical yield values for an RMA "yield trend" series.
-- Each row is one (yield_year, yield_amount) sample of a HYT identified
-- by historical_yield_trend_id. A single HYT typically carries a
-- decade-plus of historical samples used by trend-adjusted yield
-- products. CarrackYields renders these as the per-offer historical
-- chart in Mode C.
--
-- Source: 2026_A01115_HistoricalYieldTrend_YTD.txt (RMA ADM YTD release).
-- Bundle siblings: A01120 (area_data_sources), A01125 (production_areas).
-- area_data_source_id and production_area_id are denormalized onto each
-- HYT row — they are constant per historical_yield_trend_id and are
-- repeated on every (yield_year) sample.
--
-- Natural key: (reinsurance_year, historical_yield_trend_id, yield_year).
-- Verified from the file (no dupes on this tuple across 623,577 rows).
--
-- yield_amount / trended_yield_amount / detrended_yield_amount are
-- stored as numeric — the file values are always clean decimals
-- (e.g. "2427.00") and downstream consumers (CarrackYields chart code)
-- want numbers, not strings. yield_year is smallint to match the
-- reinsurance_year/crop_year convention.
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.historical_yield_trend (
  id                          bigserial      PRIMARY KEY,
  reinsurance_year            smallint       NOT NULL,
  historical_yield_trend_id   varchar(10)    NOT NULL,
  yield_year                  smallint       NOT NULL,
  yield_amount                numeric(12,4),
  trended_yield_amount        numeric(12,4),
  detrended_yield_amount      numeric(12,4),
  area_data_source_id         varchar(10),
  production_area_id          varchar(10),
  source_file                 varchar(200),
  ingested_at                 timestamptz    DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS historical_yield_trend_natural_key
  ON public.historical_yield_trend (reinsurance_year, historical_yield_trend_id, yield_year);

-- Lookup index for the FK from insurance_offers.historical_yield_trend_id —
-- the most common query is "give me all years for this HYT_id".
CREATE INDEX IF NOT EXISTS historical_yield_trend_hyt_id
  ON public.historical_yield_trend (historical_yield_trend_id);
