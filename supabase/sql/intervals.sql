-- ============================================================
-- intervals — A00480 lookup table
--
-- Resolves interval_code (raw values like "001", "631") to human
-- names ("Harvest Period 1") and abbreviations. Used by CarrackYields
-- to render the interval dimension in panel readouts and dropdowns
-- where it currently shows raw codes.
--
-- Source: 2026_A00480_Interval_YTD.txt (RMA ADM YTD release).
-- Natural key is (reinsurance_year, interval_code), enforced as a
-- separate unique index so id can serve as a stable surrogate key
-- (subsidy_percents pattern, not the older practice_types pattern).
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.intervals (
  id                    bigserial   PRIMARY KEY,
  reinsurance_year      smallint    NOT NULL,
  interval_code         varchar(3)  NOT NULL,
  interval_name         varchar(100),
  interval_abbreviation varchar(20),
  interval_start_date   varchar(8),
  interval_end_date     varchar(8)
);

CREATE UNIQUE INDEX IF NOT EXISTS intervals_natural_key
  ON public.intervals (reinsurance_year, interval_code);
