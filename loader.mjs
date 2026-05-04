/**
 * Standalone data loader — runs outside Next.js for stability.
 * Usage: node loader.mjs <path-to-txt-file>
 * Example: node loader.mjs data/adm/2026/ytd/2026_A00810_Price_YTD.txt
 *
 * Supports checkpoints — if it crashes, re-run the same command and it picks up where it left off.
 */

import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// Load .env.local
dotenv.config({ path: '.env.local' });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env.local');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const BATCH_SIZE = 2000;
const CHECKPOINT_DIR = './data/.checkpoints';

// ============================================================
// Record configs (same as parser.ts)
// ============================================================

function makeNumericSet(cols) {
  return new Set(cols);
}

const RECORD_CONFIGS = {
  A00520: { table: 'states', columns: ['record_type_code','record_category_code','reinsurance_year','state_code','state_name','state_abbreviation','regional_office_code','regional_office_name','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'state_code', isCore: false },
  A00440: { table: 'counties', columns: ['record_type_code','record_category_code','reinsurance_year','state_code','county_code','county_name','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'state_code,county_code', isCore: false },
  A00420: { table: 'commodities', columns: ['record_type_code','record_category_code','reinsurance_year','commodity_year','commodity_code','commodity_name','commodity_abbreviation','annual_planting_code','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year','commodity_year']), conflictColumns: 'commodity_code,commodity_year', isCore: false },
  A00460: { table: 'insurance_plans', columns: ['record_type_code','record_category_code','reinsurance_year','plan_code','plan_name','plan_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'plan_code', isCore: false },
  A00510: { table: 'practice_types', columns: ['record_type_code','record_category_code','reinsurance_year','commodity_code','practice_code','practice_name','practice_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'commodity_code,practice_code', isCore: false },
  A00540: { table: 'type_codes', columns: ['record_type_code','record_category_code','reinsurance_year','commodity_code','type_code','type_name','type_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'commodity_code,type_code', isCore: false },
  A00410: { table: 'classes', columns: ['record_type_code','record_category_code','reinsurance_year','class_code','class_name','class_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'class_code', isCore: false },
  A00430: { table: 'commodity_types', columns: ['record_type_code','record_category_code','reinsurance_year','commodity_type_code','commodity_type_name','commodity_type_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'commodity_type_code', isCore: false },
  A00470: { table: 'intended_uses', columns: ['record_type_code','record_category_code','reinsurance_year','intended_use_code','intended_use_name','intended_use_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'intended_use_code', isCore: false },
  A00490: { table: 'irrigation_practices', columns: ['record_type_code','record_category_code','reinsurance_year','irrigation_practice_code','irrigation_practice_name','irrigation_practice_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'irrigation_practice_code', isCore: false },
  A00450: { table: 'cropping_practices', columns: ['record_type_code','record_category_code','reinsurance_year','cropping_practice_code','cropping_practice_name','cropping_practice_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'cropping_practice_code', isCore: false },
  A00500: { table: 'organic_practices', columns: ['record_type_code','record_category_code','reinsurance_year','organic_practice_code','organic_practice_name','organic_practice_abbreviation','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'organic_practice_code', isCore: false },
  A00070: { table: 'subsidy_percents', columns: ['record_type_code','record_category_code','reinsurance_year','commodity_code','unit_structure_code','insurance_plan_code','coverage_level_percent','coverage_type_code','deductible_amount','endorsement_length_code','endorsement_length_count','insurance_option_code','range_type_code','range_low_value','range_high_value','subsidy_percent','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year','coverage_level_percent','deductible_amount','endorsement_length_count','range_low_value','range_high_value','subsidy_percent']), conflictColumns: 'reinsurance_year,commodity_code,unit_structure_code,insurance_plan_code,coverage_level_percent,coverage_type_code,insurance_option_code', isCore: true },
  A00030: { table: 'insurance_offers', columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','type_code','practice_code','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','unit_of_measure_abbreviation','program_type_code','beta_id','quality_id','unit_discount_id','historical_yield_trend_id','draw_id','optional_unit_allowed_flag','basic_unit_allowed_flag','enterprise_unit_allowed_flag','whole_farm_unit_allowed_flag','type_practice_use_code','private_508h_flag','hip_rate_id','pace_date_id','pace_rate_id','last_released_date','released_date','deleted_date','filing_date'], numericColumns: makeNumericSet(['reinsurance_year','commodity_year']), conflictColumns: 'adm_insurance_offer_id', isCore: true },
  A00810: { table: 'adm_prices', columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','crush_district_number','type_code','practice_code','insurance_option_code','range_class_code','coverage_level_percent','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','catastrophic_price','established_price','additional_price','season_average_price','contract_price_code','maximum_contract_price','maximum_over_price_election','maximum_contract_price_factor','county_base_value','projected_price','harvest_price','price_volatility_factor','allowable_cost_price','maximum_protection_per_acre','catastrophic_dollar_amount','reference_maximum_dollar_amount','density_low_quantity','density_high_quantity','age_low_count','age_high_count','minimum_dollar_amount','maximum_dollar_amount','harvest_revenue_option_factor','sucrose_factor','survival_percent','minimum_acre_percent','maximum_acre_percent','additional_value_price','maximum_additional_value_price','certified_seed_price','hybrid_seed_option_price','fixed_coverage_amount','growth_stage_code','growth_stage_factor','expected_revenue_factor','expected_county_landing_adjustment_factor','minimum_value_price','harvest_cost_amount','post_production_cost_amount','fresh_fruit_factor','expected_margin_amount','final_margin_amount','expected_index_value','final_index_value','expected_revenue_amount','final_revenue_amount','average_index_value','maximum_over_established_price','harvest_cost_amount_hand','harvest_cost_amount_machine','harvest_price_released_date','base_weight','projected_price_adjustment_factor','harvest_price_adjustment_factor','price_factor','last_released_date','released_date','deleted_date','filing_date'], numericColumns: makeNumericSet(['reinsurance_year','commodity_year','catastrophic_price','established_price','additional_price','season_average_price','maximum_contract_price','maximum_over_price_election','maximum_contract_price_factor','county_base_value','projected_price','harvest_price','price_volatility_factor','allowable_cost_price','maximum_protection_per_acre','catastrophic_dollar_amount','reference_maximum_dollar_amount','density_low_quantity','density_high_quantity','age_low_count','age_high_count','minimum_dollar_amount','maximum_dollar_amount','harvest_revenue_option_factor','sucrose_factor','survival_percent','minimum_acre_percent','maximum_acre_percent','additional_value_price','maximum_additional_value_price','certified_seed_price','hybrid_seed_option_price','fixed_coverage_amount','growth_stage_factor','expected_revenue_factor','expected_county_landing_adjustment_factor','minimum_value_price','harvest_cost_amount','post_production_cost_amount','fresh_fruit_factor','expected_margin_amount','final_margin_amount','expected_index_value','final_index_value','expected_revenue_amount','final_revenue_amount','average_index_value','maximum_over_established_price','harvest_cost_amount_hand','harvest_cost_amount_machine','base_weight','projected_price_adjustment_factor','harvest_price_adjustment_factor','price_factor']), conflictColumns: 'adm_insurance_offer_id,released_date', isCore: true },
  A00200: { table: 'adm_dates', columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','type_code','practice_code','insurance_option_code','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','contract_change_date','sales_closing_date','modified_sales_closing_date','extended_sales_closing_date','earliest_planting_date','final_planting_date','extended_final_planting_date','acreage_reporting_date','modified_acreage_reporting_date','end_of_insurance_date','cancellation_date','modified_cancellation_date','termination_date','premium_billing_date','production_reporting_date','modified_production_reporting_date','end_of_late_planting_period_date','sales_period_begin_date','sales_period_end_date','insurance_attachment_date','commodity_reporting_date','modified_commodity_reporting_date','insured_production_reporting_date','modified_insured_production_reporting_date','last_released_date','released_date','deleted_date','filing_date'], numericColumns: makeNumericSet(['reinsurance_year','commodity_year']), conflictColumns: 'adm_insurance_offer_id,released_date', isCore: true },
  A01010: { table: 'base_rates', columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','type_code','practice_code','range_class_code','coverage_level_percent','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','county_yield','density_low_quantity','density_high_quantity','reference_amount','reference_amount_code','reference_rate','exponent_value','fixed_rate','prior_year_reference_amount','prior_year_reference_rate','prior_year_exponent_value','prior_year_fixed_rate','base_rate','prior_year_base_rate','last_released_date','released_date','deleted_date','filing_date'], numericColumns: makeNumericSet(['reinsurance_year','commodity_year','coverage_level_percent','county_yield','density_low_quantity','density_high_quantity','reference_amount','reference_rate','exponent_value','fixed_rate','prior_year_reference_amount','prior_year_reference_rate','prior_year_exponent_value','prior_year_fixed_rate','base_rate','prior_year_base_rate']), conflictColumns: 'adm_insurance_offer_id,released_date', isCore: true },
  A01100: { table: 'yields', columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','type_code','practice_code','transitional_amount_code','leaf_year','characteristic_code','density_low_quantity','density_high_quantity','prior_commodity_year','wa_number','wa_land_id','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','transitional_amount','prior_transitional_amount','characteristic_name','last_reported_leaf_year','transitional_amount_uom_abbreviation','last_released_date','released_date','deleted_date','filing_date'], numericColumns: makeNumericSet(['reinsurance_year','commodity_year','density_low_quantity','density_high_quantity','transitional_amount','prior_transitional_amount']), conflictColumns: 'adm_insurance_offer_id,transitional_amount_code,leaf_year,released_date', isCore: true },
  A01115: { table: 'historical_yield_trend', columns: ['record_type_code','record_category_code','reinsurance_year','historical_yield_trend_id','yield_year','yield_amount','trended_yield_amount','detrended_yield_amount','area_data_source_id','production_area_id','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year','yield_year','yield_amount','trended_yield_amount','detrended_yield_amount']), conflictColumns: 'reinsurance_year,historical_yield_trend_id,yield_year', isCore: true },
  A01120: { table: 'area_data_sources', columns: ['record_type_code','record_category_code','reinsurance_year','area_data_source_id','commodity_code','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','area_basis_code','area_source_code','index_value_code','yield_conversion_factor','rate_method_code','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'reinsurance_year,area_data_source_id', isCore: true },
  A01125: { table: 'production_areas', columns: ['record_type_code','record_category_code','reinsurance_year','production_area_id','state_code','county_code','production_area_state_code','production_area_county_code','last_released_date','released_date','deleted_date'], numericColumns: makeNumericSet(['reinsurance_year']), conflictColumns: 'reinsurance_year,production_area_id,state_code,county_code,production_area_state_code,production_area_county_code', isCore: true },
};

// ============================================================
// Checkpoint support
// ============================================================

function getCheckpointPath(filePath) {
  const key = path.basename(filePath).replace(/[^a-zA-Z0-9_.-]/g, '_');
  return path.join(CHECKPOINT_DIR, `${key}.json`);
}

function readCheckpoint(filePath) {
  const cpPath = getCheckpointPath(filePath);
  if (!fs.existsSync(cpPath)) return null;
  try {
    return JSON.parse(fs.readFileSync(cpPath, 'utf-8'));
  } catch {
    return null;
  }
}

function writeCheckpoint(filePath, lineNumber, rowsUpserted) {
  if (!fs.existsSync(CHECKPOINT_DIR)) fs.mkdirSync(CHECKPOINT_DIR, { recursive: true });
  fs.writeFileSync(getCheckpointPath(filePath), JSON.stringify({ lineNumber, rowsUpserted, updatedAt: new Date().toISOString() }));
}

function clearCheckpoint(filePath) {
  const cpPath = getCheckpointPath(filePath);
  if (fs.existsSync(cpPath)) fs.unlinkSync(cpPath);
}

// ============================================================
// Row parser
// ============================================================

// Columns that must be '' instead of null (part of unique constraints)
const EMPTY_STRING_COLS = new Set([
  'transitional_amount_code', 'leaf_year',  // yields
  'commodity_code', 'insurance_option_code', // subsidy_percents
]);

function parseRow(values, config, sourceLabel) {
  const row = {};

  if (config.isCore && ['insurance_offers','adm_prices','adm_dates','base_rates','yields','historical_yield_trend','area_data_sources','production_areas'].includes(config.table)) {
    row.source_file = sourceLabel;
  }

  for (let j = 0; j < config.columns.length && j < values.length; j++) {
    const col = config.columns[j];
    const val = values[j]?.trim() ?? '';

    if (col === 'record_type_code' || col === 'record_category_code' ||
        col === 'last_released_date' || col === 'released_date' || col === 'deleted_date') {
      if (col === 'released_date' || col === 'deleted_date') {
        if (['insurance_offers','adm_prices','adm_dates','base_rates','yields','subsidy_percents'].includes(config.table)) {
          row[col] = val || null;
        }
      }
      if (!['insurance_offers','adm_prices','adm_dates','base_rates','yields','subsidy_percents'].includes(config.table)) {
        continue;
      }
      if (col === 'record_type_code' || col === 'record_category_code' || col === 'last_released_date') {
        continue;
      }
    }

    if (val === '') {
      // For columns that participate in unique constraints, use '' instead of null
      // (only applies to the tables that have these constraints)
      if (EMPTY_STRING_COLS.has(col) && (config.table === 'yields' || config.table === 'subsidy_percents')) {
        row[col] = '';
      } else {
        row[col] = null;
      }
    } else if (config.numericColumns.has(col)) {
      const num = parseFloat(val);
      row[col] = isNaN(num) ? null : num;
    } else {
      row[col] = val;
    }
  }

  return row;
}

// ============================================================
// Upsert
// ============================================================

async function upsertBatch(table, rows, conflictColumns) {
  const { error } = await supabase
    .from(table)
    .upsert(rows, { onConflict: conflictColumns });

  if (error) {
    return { upserted: 0, error: error.message };
  }
  return { upserted: rows.length, error: null };
}

// ============================================================
// Main
// ============================================================

async function main() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node loader.mjs <path-to-txt-file>');
    console.error('Example: node loader.mjs data/adm/2026/ytd/2026_A00810_Price_YTD.txt');
    process.exit(1);
  }

  if (!fs.existsSync(filePath)) {
    console.error(`File not found: ${filePath}`);
    process.exit(1);
  }

  const filename = path.basename(filePath);
  const match = filename.match(/A(\d{5})/);
  if (!match) {
    console.error(`Could not detect record type from filename: ${filename}`);
    process.exit(1);
  }

  const recordType = `A${match[1]}`;
  const config = RECORD_CONFIGS[recordType];
  if (!config) {
    console.error(`Unsupported record type: ${recordType}`);
    process.exit(1);
  }

  console.log(`\n========================================`);
  console.log(`  Loading: ${filename}`);
  console.log(`  Table:   ${config.table}`);
  console.log(`  Record:  ${recordType}`);
  console.log(`========================================\n`);

  // Check checkpoint
  const checkpoint = readCheckpoint(filePath);
  const skipToLine = checkpoint?.lineNumber ?? 0;
  let rowsUpserted = checkpoint?.rowsUpserted ?? 0;

  if (skipToLine > 0) {
    console.log(`  Resuming from line ${skipToLine.toLocaleString()} (${rowsUpserted.toLocaleString()} already upserted)\n`);
  }

  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let currentLine = 0;
  let rowsProcessed = skipToLine;
  let batch = [];
  let errors = 0;
  const startTime = Date.now();
  let lastLog = Date.now();

  for await (const line of rl) {
    currentLine++;

    if (currentLine === 1) continue; // header
    if (currentLine <= skipToLine) continue; // already done

    const trimmed = line.trim();
    if (!trimmed) continue;

    const values = trimmed.split('|');
    const row = parseRow(values, config, filename);
    batch.push(row);
    rowsProcessed++;

    if (batch.length >= BATCH_SIZE) {
      // Deduplicate batch by conflict columns to avoid "cannot affect row a second time"
      const conflictCols = config.conflictColumns.split(',');
      const seen = new Map();
      for (const row of batch) {
        const key = conflictCols.map(c => row[c] ?? '').join('|');
        seen.set(key, row); // last one wins
      }
      const dedupedBatch = Array.from(seen.values());

      const result = await upsertBatch(config.table, dedupedBatch, config.conflictColumns);
      if (result.error) {
        errors++;
        if (errors <= 5) console.error(`  Batch error: ${result.error}`);
      } else {
        rowsUpserted += result.upserted;
      }
      batch = [];

      // Checkpoint every batch
      writeCheckpoint(filePath, currentLine, rowsUpserted);

      // Log progress every 5 seconds
      const now = Date.now();
      if (now - lastLog > 5000) {
        const elapsed = ((now - startTime) / 1000).toFixed(0);
        const rate = Math.round(rowsProcessed / (elapsed || 1));
        process.stdout.write(`\r  Processed: ${rowsProcessed.toLocaleString()} | Upserted: ${rowsUpserted.toLocaleString()} | ${rate}/s | ${elapsed}s elapsed`);
        lastLog = now;
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    const conflictCols = config.conflictColumns.split(',');
    const seen = new Map();
    for (const row of batch) {
      const key = conflictCols.map(c => row[c] ?? '').join('|');
      seen.set(key, row);
    }
    batch = Array.from(seen.values());
    const result = await upsertBatch(config.table, batch, config.conflictColumns);
    if (result.error) {
      errors++;
      console.error(`  Final batch error: ${result.error}`);
    } else {
      rowsUpserted += result.upserted;
    }
  }

  // Clear checkpoint on success
  clearCheckpoint(filePath);

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n\n========================================`);
  console.log(`  DONE!`);
  console.log(`  Rows processed: ${rowsProcessed.toLocaleString()}`);
  console.log(`  Rows upserted:  ${rowsUpserted.toLocaleString()}`);
  console.log(`  Errors:         ${errors}`);
  console.log(`  Time:           ${totalTime}s`);
  console.log(`========================================\n`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
