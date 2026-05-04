-- ============================================================
-- area_data_sources — A01120
--
-- Methodology / source definitions for historical_yield_trend rows.
-- Each row defines one (commodity + intended_use + irrigation +
-- cropping + interval + area basis + area source + index value +
-- yield conversion + rate method) combination identified by
-- area_data_source_id. HYT rows reference this id via
-- area_data_source_id to declare "which methodology was used to
-- produce these yield values".
--
-- Source: 2026_A01120_AreaDataSource_YTD.txt (RMA ADM YTD release).
-- Bundle siblings: A01115 (historical_yield_trend), A01125
-- (production_areas).
-- Natural key: (reinsurance_year, area_data_source_id) — verified
-- unique across all 99 rows in the file.
--
-- All discriminator codes are varchar per the CarrackRMA convention
-- (matches the existing lookup-table pattern for codes that are
-- themselves looked up against state/commodity/practice tables).
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.area_data_sources (
  id                       bigserial    PRIMARY KEY,
  reinsurance_year         smallint     NOT NULL,
  area_data_source_id      varchar(10)  NOT NULL,
  commodity_code           varchar(4),
  commodity_type_code      varchar(3),
  class_code               varchar(3),
  sub_class_code           varchar(3),
  intended_use_code        varchar(3),
  irrigation_practice_code varchar(3),
  cropping_practice_code   varchar(3),
  organic_practice_code    varchar(3),
  interval_code            varchar(3),
  area_basis_code          varchar(3),
  area_source_code         varchar(3),
  index_value_code         varchar(3),
  yield_conversion_factor  varchar(20),
  rate_method_code         varchar(3),
  source_file              varchar(200),
  ingested_at              timestamptz  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS area_data_sources_natural_key
  ON public.area_data_sources (reinsurance_year, area_data_source_id);
