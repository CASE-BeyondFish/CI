-- ============================================================
-- Yields Database Schema (v2)
-- Run this in Supabase SQL Editor
-- DROP old tables first if they exist from v1
-- ============================================================

DROP TABLE IF EXISTS adm_prices CASCADE;
DROP TABLE IF EXISTS ingestion_log CASCADE;
DROP TABLE IF EXISTS states CASCADE;
DROP TABLE IF EXISTS counties CASCADE;
DROP TABLE IF EXISTS commodities CASCADE;
DROP TABLE IF EXISTS insurance_plans CASCADE;
DROP TABLE IF EXISTS practice_types CASCADE;
DROP TABLE IF EXISTS type_codes CASCADE;

-- ============================================================
-- Lookup / Reference Tables
-- ============================================================

CREATE TABLE states (
  state_code VARCHAR(2) PRIMARY KEY,
  state_name VARCHAR(100),
  state_abbreviation VARCHAR(10),
  regional_office_code VARCHAR(2),
  regional_office_name VARCHAR(50),
  reinsurance_year SMALLINT
);

CREATE TABLE counties (
  state_code VARCHAR(2) NOT NULL,
  county_code VARCHAR(3) NOT NULL,
  county_name VARCHAR(100),
  reinsurance_year SMALLINT,
  PRIMARY KEY (state_code, county_code)
);

CREATE TABLE commodities (
  commodity_code VARCHAR(4) NOT NULL,
  commodity_year SMALLINT NOT NULL,
  commodity_name VARCHAR(50),
  commodity_abbreviation VARCHAR(10),
  annual_planting_code VARCHAR(1),
  reinsurance_year SMALLINT,
  PRIMARY KEY (commodity_code, commodity_year)
);

CREATE TABLE insurance_plans (
  plan_code VARCHAR(2) PRIMARY KEY,
  plan_name VARCHAR(200),
  plan_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE practice_types (
  commodity_code VARCHAR(4) NOT NULL,
  practice_code VARCHAR(3) NOT NULL,
  practice_name VARCHAR(100),
  practice_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT,
  PRIMARY KEY (commodity_code, practice_code)
);

CREATE TABLE type_codes (
  commodity_code VARCHAR(4) NOT NULL,
  type_code VARCHAR(3) NOT NULL,
  type_name VARCHAR(100),
  type_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT,
  PRIMARY KEY (commodity_code, type_code)
);

CREATE TABLE classes (
  class_code VARCHAR(3) PRIMARY KEY,
  class_name VARCHAR(50),
  class_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE commodity_types (
  commodity_type_code VARCHAR(3) PRIMARY KEY,
  commodity_type_name VARCHAR(50),
  commodity_type_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE intended_uses (
  intended_use_code VARCHAR(3) PRIMARY KEY,
  intended_use_name VARCHAR(50),
  intended_use_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE irrigation_practices (
  irrigation_practice_code VARCHAR(3) PRIMARY KEY,
  irrigation_practice_name VARCHAR(50),
  irrigation_practice_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE cropping_practices (
  cropping_practice_code VARCHAR(3) PRIMARY KEY,
  cropping_practice_name VARCHAR(50),
  cropping_practice_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE organic_practices (
  organic_practice_code VARCHAR(3) PRIMARY KEY,
  organic_practice_name VARCHAR(50),
  organic_practice_abbreviation VARCHAR(20),
  reinsurance_year SMALLINT
);

CREATE TABLE subsidy_percents (
  id BIGSERIAL PRIMARY KEY,
  reinsurance_year SMALLINT,
  commodity_code VARCHAR(4),
  unit_structure_code VARCHAR(2),
  insurance_plan_code VARCHAR(2),
  coverage_level_percent NUMERIC(4,2),
  coverage_type_code VARCHAR(1),
  deductible_amount NUMERIC(12,4),
  endorsement_length_code VARCHAR(3),
  endorsement_length_count NUMERIC(5,0),
  insurance_option_code VARCHAR(2),
  range_type_code VARCHAR(3),
  range_low_value NUMERIC(12,4),
  range_high_value NUMERIC(12,4),
  subsidy_percent NUMERIC(5,3)
);
CREATE UNIQUE INDEX idx_subsidy_unique ON subsidy_percents
  (reinsurance_year, commodity_code, unit_structure_code, insurance_plan_code,
   coverage_level_percent, coverage_type_code, insurance_option_code);

-- ============================================================
-- Core Data Tables
-- ============================================================

-- A00030: Insurance Offer — the master record
CREATE TABLE insurance_offers (
  adm_insurance_offer_id VARCHAR(20) PRIMARY KEY,
  record_type_code VARCHAR(6),
  record_category_code VARCHAR(2),
  reinsurance_year SMALLINT,
  commodity_year SMALLINT,
  commodity_code VARCHAR(4),
  insurance_plan_code VARCHAR(2),
  state_code VARCHAR(2),
  county_code VARCHAR(3),
  type_code VARCHAR(3),
  practice_code VARCHAR(3),
  wa_number VARCHAR(9),
  commodity_type_code VARCHAR(3),
  class_code VARCHAR(3),
  sub_class_code VARCHAR(3),
  intended_use_code VARCHAR(3),
  irrigation_practice_code VARCHAR(3),
  cropping_practice_code VARCHAR(3),
  organic_practice_code VARCHAR(3),
  interval_code VARCHAR(3),
  unit_of_measure_abbreviation VARCHAR(5),
  program_type_code VARCHAR(1),
  beta_id VARCHAR(10),
  quality_id VARCHAR(10),
  unit_discount_id VARCHAR(10),
  historical_yield_trend_id VARCHAR(10),
  draw_id VARCHAR(10),
  optional_unit_allowed_flag VARCHAR(1),
  basic_unit_allowed_flag VARCHAR(1),
  enterprise_unit_allowed_flag VARCHAR(1),
  whole_farm_unit_allowed_flag VARCHAR(1),
  type_practice_use_code VARCHAR(1),
  private_508h_flag VARCHAR(1),
  hip_rate_id VARCHAR(10),
  pace_date_id VARCHAR(10),
  pace_rate_id VARCHAR(10),
  last_released_date VARCHAR(8),
  released_date VARCHAR(8),
  deleted_date VARCHAR(8),
  filing_date VARCHAR(8),
  source_file VARCHAR(200),
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_offers_lookup ON insurance_offers (state_code, county_code, commodity_code, commodity_year);
CREATE INDEX idx_offers_plan ON insurance_offers (insurance_plan_code);

-- A00810: Price
CREATE TABLE adm_prices (
  id BIGSERIAL PRIMARY KEY,
  adm_insurance_offer_id VARCHAR(20),
  record_type_code VARCHAR(6),
  record_category_code VARCHAR(2),
  reinsurance_year SMALLINT,
  commodity_year SMALLINT,
  commodity_code VARCHAR(4),
  insurance_plan_code VARCHAR(2),
  state_code VARCHAR(2),
  county_code VARCHAR(3),
  sub_county_code VARCHAR(10),
  crush_district_number VARCHAR(10),
  type_code VARCHAR(3),
  practice_code VARCHAR(3),
  insurance_option_code VARCHAR(2),
  range_class_code VARCHAR(3),
  coverage_level_percent VARCHAR(10),
  wa_number VARCHAR(9),
  commodity_type_code VARCHAR(3),
  class_code VARCHAR(3),
  sub_class_code VARCHAR(3),
  intended_use_code VARCHAR(3),
  irrigation_practice_code VARCHAR(3),
  cropping_practice_code VARCHAR(3),
  organic_practice_code VARCHAR(3),
  interval_code VARCHAR(3),
  catastrophic_price NUMERIC(12,4),
  established_price NUMERIC(12,4),
  additional_price NUMERIC(12,4),
  season_average_price NUMERIC(12,4),
  contract_price_code VARCHAR(10),
  maximum_contract_price NUMERIC(12,4),
  maximum_over_price_election NUMERIC(12,4),
  maximum_contract_price_factor NUMERIC(12,4),
  county_base_value NUMERIC(12,4),
  projected_price NUMERIC(12,4),
  harvest_price NUMERIC(12,4),
  price_volatility_factor NUMERIC(12,4),
  allowable_cost_price NUMERIC(12,4),
  maximum_protection_per_acre NUMERIC(12,4),
  catastrophic_dollar_amount NUMERIC(12,4),
  reference_maximum_dollar_amount NUMERIC(12,4),
  density_low_quantity NUMERIC(12,4),
  density_high_quantity NUMERIC(12,4),
  age_low_count NUMERIC(12,4),
  age_high_count NUMERIC(12,4),
  minimum_dollar_amount NUMERIC(12,4),
  maximum_dollar_amount NUMERIC(12,4),
  harvest_revenue_option_factor NUMERIC(12,4),
  sucrose_factor NUMERIC(12,4),
  survival_percent NUMERIC(12,4),
  minimum_acre_percent NUMERIC(12,4),
  maximum_acre_percent NUMERIC(12,4),
  additional_value_price NUMERIC(12,4),
  maximum_additional_value_price NUMERIC(12,4),
  certified_seed_price NUMERIC(12,4),
  hybrid_seed_option_price NUMERIC(12,4),
  fixed_coverage_amount NUMERIC(12,4),
  growth_stage_code VARCHAR(10),
  growth_stage_factor NUMERIC(12,4),
  expected_revenue_factor NUMERIC(12,4),
  expected_county_landing_adjustment_factor NUMERIC(12,4),
  minimum_value_price NUMERIC(12,4),
  harvest_cost_amount NUMERIC(12,4),
  post_production_cost_amount NUMERIC(12,4),
  fresh_fruit_factor NUMERIC(12,4),
  expected_margin_amount NUMERIC(12,4),
  final_margin_amount NUMERIC(12,4),
  expected_index_value NUMERIC(12,4),
  final_index_value NUMERIC(12,4),
  expected_revenue_amount NUMERIC(12,4),
  final_revenue_amount NUMERIC(12,4),
  average_index_value NUMERIC(12,4),
  maximum_over_established_price NUMERIC(12,4),
  harvest_cost_amount_hand NUMERIC(12,4),
  harvest_cost_amount_machine NUMERIC(12,4),
  harvest_price_released_date VARCHAR(8),
  base_weight NUMERIC(12,4),
  projected_price_adjustment_factor NUMERIC(12,4),
  harvest_price_adjustment_factor NUMERIC(12,4),
  price_factor NUMERIC(12,4),
  last_released_date VARCHAR(8),
  released_date VARCHAR(8),
  deleted_date VARCHAR(8),
  filing_date VARCHAR(8),
  source_file VARCHAR(200),
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_prices_offer ON adm_prices (adm_insurance_offer_id, released_date);
CREATE INDEX idx_prices_lookup ON adm_prices (state_code, county_code, commodity_code, commodity_year);

-- A00200: Dates
CREATE TABLE adm_dates (
  id BIGSERIAL PRIMARY KEY,
  adm_insurance_offer_id VARCHAR(20),
  record_type_code VARCHAR(6),
  record_category_code VARCHAR(2),
  reinsurance_year SMALLINT,
  commodity_year SMALLINT,
  commodity_code VARCHAR(4),
  insurance_plan_code VARCHAR(2),
  state_code VARCHAR(2),
  county_code VARCHAR(3),
  sub_county_code VARCHAR(10),
  type_code VARCHAR(3),
  practice_code VARCHAR(3),
  insurance_option_code VARCHAR(2),
  wa_number VARCHAR(9),
  commodity_type_code VARCHAR(3),
  class_code VARCHAR(3),
  sub_class_code VARCHAR(3),
  intended_use_code VARCHAR(3),
  irrigation_practice_code VARCHAR(3),
  cropping_practice_code VARCHAR(3),
  organic_practice_code VARCHAR(3),
  interval_code VARCHAR(3),
  contract_change_date VARCHAR(8),
  sales_closing_date VARCHAR(8),
  modified_sales_closing_date VARCHAR(8),
  extended_sales_closing_date VARCHAR(8),
  earliest_planting_date VARCHAR(8),
  final_planting_date VARCHAR(8),
  extended_final_planting_date VARCHAR(8),
  acreage_reporting_date VARCHAR(8),
  modified_acreage_reporting_date VARCHAR(8),
  end_of_insurance_date VARCHAR(8),
  cancellation_date VARCHAR(8),
  modified_cancellation_date VARCHAR(8),
  termination_date VARCHAR(8),
  premium_billing_date VARCHAR(8),
  production_reporting_date VARCHAR(8),
  modified_production_reporting_date VARCHAR(8),
  end_of_late_planting_period_date VARCHAR(8),
  sales_period_begin_date VARCHAR(8),
  sales_period_end_date VARCHAR(8),
  insurance_attachment_date VARCHAR(8),
  commodity_reporting_date VARCHAR(8),
  modified_commodity_reporting_date VARCHAR(8),
  insured_production_reporting_date VARCHAR(8),
  modified_insured_production_reporting_date VARCHAR(8),
  last_released_date VARCHAR(8),
  released_date VARCHAR(8),
  deleted_date VARCHAR(8),
  filing_date VARCHAR(8),
  source_file VARCHAR(200),
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_dates_offer ON adm_dates (adm_insurance_offer_id, released_date);
CREATE INDEX idx_dates_lookup ON adm_dates (state_code, county_code, commodity_code, commodity_year);

-- A01010: Base Rate
CREATE TABLE base_rates (
  id BIGSERIAL PRIMARY KEY,
  adm_insurance_offer_id VARCHAR(20),
  record_type_code VARCHAR(6),
  record_category_code VARCHAR(2),
  reinsurance_year SMALLINT,
  commodity_year SMALLINT,
  commodity_code VARCHAR(4),
  insurance_plan_code VARCHAR(2),
  state_code VARCHAR(2),
  county_code VARCHAR(3),
  sub_county_code VARCHAR(10),
  type_code VARCHAR(3),
  practice_code VARCHAR(3),
  range_class_code VARCHAR(3),
  coverage_level_percent NUMERIC(4,2),
  wa_number VARCHAR(9),
  commodity_type_code VARCHAR(3),
  class_code VARCHAR(3),
  sub_class_code VARCHAR(3),
  intended_use_code VARCHAR(3),
  irrigation_practice_code VARCHAR(3),
  cropping_practice_code VARCHAR(3),
  organic_practice_code VARCHAR(3),
  interval_code VARCHAR(3),
  county_yield NUMERIC(8,2),
  density_low_quantity NUMERIC(7,0),
  density_high_quantity NUMERIC(7,0),
  reference_amount NUMERIC(8,2),
  reference_amount_code VARCHAR(1),
  reference_rate NUMERIC(6,4),
  exponent_value NUMERIC(7,3),
  fixed_rate NUMERIC(6,4),
  prior_year_reference_amount NUMERIC(8,2),
  prior_year_reference_rate NUMERIC(6,4),
  prior_year_exponent_value NUMERIC(7,3),
  prior_year_fixed_rate NUMERIC(6,4),
  base_rate NUMERIC(11,4),
  prior_year_base_rate NUMERIC(11,4),
  last_released_date VARCHAR(8),
  released_date VARCHAR(8),
  deleted_date VARCHAR(8),
  filing_date VARCHAR(8),
  source_file VARCHAR(200),
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_baserate_offer ON base_rates (adm_insurance_offer_id, released_date);
CREATE INDEX idx_baserate_lookup ON base_rates (state_code, county_code, commodity_code, commodity_year);

-- A01100: Yield and T-Yield
CREATE TABLE yields (
  id BIGSERIAL PRIMARY KEY,
  adm_insurance_offer_id VARCHAR(20),
  record_type_code VARCHAR(6),
  record_category_code VARCHAR(2),
  reinsurance_year SMALLINT,
  commodity_year SMALLINT,
  commodity_code VARCHAR(4),
  insurance_plan_code VARCHAR(2),
  state_code VARCHAR(2),
  county_code VARCHAR(3),
  sub_county_code VARCHAR(10),
  type_code VARCHAR(3),
  practice_code VARCHAR(3),
  transitional_amount_code VARCHAR(1),
  leaf_year VARCHAR(4),
  characteristic_code VARCHAR(10),
  density_low_quantity NUMERIC(7,0),
  density_high_quantity NUMERIC(7,0),
  prior_commodity_year VARCHAR(4),
  wa_number VARCHAR(9),
  wa_land_id VARCHAR(10),
  commodity_type_code VARCHAR(3),
  class_code VARCHAR(3),
  sub_class_code VARCHAR(3),
  intended_use_code VARCHAR(3),
  irrigation_practice_code VARCHAR(3),
  cropping_practice_code VARCHAR(3),
  organic_practice_code VARCHAR(3),
  interval_code VARCHAR(3),
  transitional_amount NUMERIC(10,2),
  prior_transitional_amount NUMERIC(10,2),
  characteristic_name VARCHAR(50),
  last_reported_leaf_year VARCHAR(4),
  transitional_amount_uom_abbreviation VARCHAR(5),
  last_released_date VARCHAR(8),
  released_date VARCHAR(8),
  deleted_date VARCHAR(8),
  filing_date VARCHAR(8),
  source_file VARCHAR(200),
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_yields_offer ON yields
  (adm_insurance_offer_id, transitional_amount_code, leaf_year, released_date);
CREATE INDEX idx_yields_lookup ON yields (state_code, county_code, commodity_code, commodity_year);

-- ============================================================
-- Ingestion Log
-- ============================================================

CREATE TABLE ingestion_log (
  id BIGSERIAL PRIMARY KEY,
  source VARCHAR(50) NOT NULL,
  filename VARCHAR(200) NOT NULL,
  record_type VARCHAR(20),
  rows_processed INT DEFAULT 0,
  rows_upserted INT DEFAULT 0,
  status VARCHAR(20) DEFAULT 'pending',
  error_message TEXT,
  started_at TIMESTAMPTZ DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);
