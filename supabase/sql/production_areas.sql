-- ============================================================
-- production_areas — A01125
--
-- Geographic area definitions for historical_yield_trend rows. Each
-- row maps an origin (state_code, county_code) into a production-area
-- coordinate pair (production_area_state_code, production_area_county_code)
-- under a logical production_area_id. A single production_area_id is
-- composed of MULTIPLE rows — it groups several (origin → area)
-- mappings together — so production_area_id alone is NOT unique in
-- this table. Verified from the file: 12,245 production_area_ids
-- appear on multiple rows; the full tuple
-- (production_area_id, state_code, county_code,
--  production_area_state_code, production_area_county_code) is unique
-- across all 223,595 rows.
--
-- Source: 2026_A01125_ProductionArea_YTD.txt (RMA ADM YTD release).
-- Bundle siblings: A01115 (historical_yield_trend), A01120
-- (area_data_sources).
--
-- Natural key for upsert: (reinsurance_year, production_area_id,
-- state_code, county_code, production_area_state_code,
-- production_area_county_code).
--
-- A separate non-unique lookup index on production_area_id supports
-- joins from historical_yield_trend.production_area_id back into the
-- set of (state, county) constituents.
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.production_areas (
  id                            bigserial    PRIMARY KEY,
  reinsurance_year              smallint     NOT NULL,
  production_area_id            varchar(10)  NOT NULL,
  state_code                    varchar(2),
  county_code                   varchar(3),
  production_area_state_code    varchar(2),
  production_area_county_code   varchar(3),
  source_file                   varchar(200),
  ingested_at                   timestamptz  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS production_areas_natural_key
  ON public.production_areas (
    reinsurance_year,
    production_area_id,
    state_code,
    county_code,
    production_area_state_code,
    production_area_county_code
  );

CREATE INDEX IF NOT EXISTS production_areas_id
  ON public.production_areas (production_area_id);
