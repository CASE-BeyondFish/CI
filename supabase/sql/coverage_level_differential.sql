-- ============================================================
-- coverage_level_differential — A01040
--
-- Per-offer coverage-level rate composition factors. Each row is one
-- (offer, coverage_level_percent, coverage_type_code) tuple carrying
-- the factors RMA publishes to scale base rate at that coverage level:
-- rate_differential_factor, unit_residual_factor, enterprise_unit_residual_factor,
-- whole_farm_unit_residual_factor, plus prior-year and CAT siblings.
--
-- Source: 2026_A01040_CoverageLevelDifferential_YTD.txt (RMA ADM YTD release).
-- Sibling of A01010 (base_rates) and A00810 (adm_prices) in the rate
-- composition family — same offer-keyed shape, with coverage_level_percent
-- as an extra dimension. Phase 13 Part B (CarrackYields Mode C "Rate
-- Composition" tab) renders these factors as a static read-only grid.
--
-- Natural key: (adm_insurance_offer_id, coverage_level_percent,
-- coverage_type_code, released_date). released_date is included so multiple
-- snapshots of the same factor at different release dates can coexist —
-- same pattern as base_rates / adm_prices. coverage_type_code participates
-- in the key because A and C (CAT) carry distinct factor sets.
--
-- Provenance: filing_date is preserved verbatim per the standing rule —
-- power users want to see when RMA filed each row. released_date and
-- deleted_date are kept on every row (this is core data, not a lookup).
--
-- Geography lookup index covers Part B's primary query shape:
-- "for this (commodity_year, state_code, county_code, commodity_code,
-- insurance_plan_code), give me all coverage level differentials".
--
-- This file is large (~17.9M rows in 2026 YTD). The geography index makes
-- per-tuple lookups cheap; the natural-key index supports idempotent
-- upserts during ingest.
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.coverage_level_differential (
  id                                              bigserial      PRIMARY KEY,
  adm_insurance_offer_id                          varchar(20)    NOT NULL,
  reinsurance_year                                smallint,
  commodity_year                                  smallint,
  commodity_code                                  varchar(4),
  insurance_plan_code                             varchar(3),
  state_code                                      varchar(2),
  county_code                                     varchar(3),
  sub_county_code                                 varchar(3),
  type_code                                       varchar(3),
  practice_code                                   varchar(3),
  insurance_option_code                           varchar(3),
  coverage_level_percent                          numeric(5,4)   NOT NULL,
  coverage_type_code                              varchar(2)     NOT NULL,
  wa_number                                       varchar(10),
  wa_land_id                                      varchar(20),
  commodity_type_code                             varchar(3),
  class_code                                      varchar(3),
  sub_class_code                                  varchar(3),
  intended_use_code                               varchar(3),
  irrigation_practice_code                        varchar(3),
  cropping_practice_code                          varchar(3),
  organic_practice_code                           varchar(3),
  interval_code                                   varchar(3),
  rate_differential_factor                        numeric(12,9),
  unit_residual_factor                            numeric(12,4),
  enterprise_unit_residual_factor                 numeric(12,4),
  whole_farm_unit_residual_factor                 numeric(12,4),
  prior_year_rate_differential_factor             numeric(12,9),
  prior_year_unit_residual_factor                 numeric(12,4),
  prior_year_enterprise_unit_residual_factor      numeric(12,4),
  prior_year_whole_farm_unit_residual_factor      numeric(12,4),
  cat_residual_factor                             numeric(12,4),
  prior_cat_residual_factor                       numeric(12,4),
  released_date                                   varchar(8),
  deleted_date                                    varchar(8),
  filing_date                                     varchar(8),
  source_file                                     varchar(200),
  ingested_at                                     timestamptz    DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS coverage_level_differential_natural_key
  ON public.coverage_level_differential (
    adm_insurance_offer_id,
    coverage_level_percent,
    coverage_type_code,
    released_date
  );

CREATE INDEX IF NOT EXISTS coverage_level_differential_geography
  ON public.coverage_level_differential (
    commodity_year,
    state_code,
    county_code,
    commodity_code,
    insurance_plan_code
  );
