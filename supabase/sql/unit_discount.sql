-- ============================================================
-- unit_discount — A01090
--
-- Per-(unit_discount_id, coverage_level, area-band) factors that scale
-- premium by unit structure (basic / optional / enterprise). Each row
-- carries the discount factors plus the coefficient regression terms
-- RMA publishes (intercept_coefficient, total_unit_size_coefficient,
-- average_county_base_rate_coefficient, etc.) used in the underlying
-- formula. CarrackYields Mode C "Rate Composition" tab (Phase 13 Part B)
-- renders these as a static read-only grid.
--
-- Source: 2026_A01090_UnitDiscount_YTD.txt (RMA ADM YTD release).
-- insurance_offers.unit_discount_id (already populated) is the FK that
-- links an offer to a set of unit_discount rows. Geography filtering for
-- Part B happens via that join (offer carries commodity_year, state_code,
-- county_code, commodity_code, insurance_plan_code).
--
-- Natural key: (reinsurance_year, unit_discount_id, coverage_level_percent,
-- area_low_quantity, area_high_quantity). NULLS NOT DISTINCT because
-- category-02 rows (~the simple case) leave coverage_level_percent and
-- both area-band columns empty, and we want one row per unit_discount_id
-- in that case (not many "all-null" rows treated as distinct). Requires
-- Postgres 15+ — Supabase is on 15+.
--
-- Lookup-style table — no source_file / no released_date / no filing_date
-- (the file carries last_released_date and released_date for ops, but
-- they're snapshot metadata, not row identity, and aren't referenced by
-- Part B). Pattern matches subsidy_percents and intervals.
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.unit_discount (
  id                                       bigserial      PRIMARY KEY,
  reinsurance_year                         smallint       NOT NULL,
  unit_discount_id                         varchar(20)    NOT NULL,
  coverage_level_percent                   numeric(5,4),
  area_low_quantity                        numeric(15,2),
  area_high_quantity                       numeric(15,2),
  intercept_coefficient                    numeric(15,9),
  total_unit_size_coefficient              numeric(15,9),
  average_county_base_rate_coefficient     numeric(15,9),
  type_coefficient                         numeric(15,9),
  practice_coefficient                     numeric(15,9),
  commodity_type_coefficient               numeric(15,9),
  class_coefficient                        numeric(15,9),
  sub_class_coefficient                    numeric(15,9),
  intended_use_coefficient                 numeric(15,9),
  irrigation_practice_coefficient          numeric(15,9),
  cropping_practice_coefficient            numeric(15,9),
  organic_practice_coefficient             numeric(15,9),
  interval_coefficient                     numeric(15,9),
  standard_deviation_quantity              numeric(15,9),
  optional_unit_discount_factor            numeric(8,4),
  basic_unit_discount_factor               numeric(8,4),
  enterprise_unit_discount_factor          numeric(8,4),
  area_description                         varchar(100),
  ingested_at                              timestamptz    DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS unit_discount_natural_key
  ON public.unit_discount (
    reinsurance_year,
    unit_discount_id,
    coverage_level_percent,
    area_low_quantity,
    area_high_quantity
  )
  NULLS NOT DISTINCT;

-- Lookup index for the FK from insurance_offers.unit_discount_id —
-- Part B's hot path is "give me all unit_discount rows for this id".
CREATE INDEX IF NOT EXISTS unit_discount_id_lookup
  ON public.unit_discount (unit_discount_id);
