import fs from 'fs';
import path from 'path';
import AdmZip from 'adm-zip';
import { supabase } from '../db/supabase';
import { ManifestEntry } from './types';
import { readManifest, writeManifest, manifestKey } from './manifest';

const DATA_DIR = process.env.DATA_DIR || './data';
const BATCH_SIZE = 2000;

// ============================================================
// Record type → table mapping + column definitions
// ============================================================

interface RecordConfig {
  table: string;
  columns: string[];
  numericColumns: Set<string>;
  conflictColumns: string;
}

// Helper: columns that appear as numeric across many record types
function makeNumericSet(cols: string[]): Set<string> {
  return new Set(cols);
}

const RECORD_CONFIGS: Record<string, RecordConfig> = {
  // Lookup tables
  A00520: {
    table: 'states',
    columns: ['record_type_code','record_category_code','reinsurance_year','state_code','state_name','state_abbreviation','regional_office_code','regional_office_name','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'state_code',
  },
  A00440: {
    table: 'counties',
    columns: ['record_type_code','record_category_code','reinsurance_year','state_code','county_code','county_name','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'state_code,county_code',
  },
  A00420: {
    table: 'commodities',
    columns: ['record_type_code','record_category_code','reinsurance_year','commodity_year','commodity_code','commodity_name','commodity_abbreviation','annual_planting_code','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year','commodity_year']),
    conflictColumns: 'commodity_code,commodity_year',
  },
  A00460: {
    table: 'insurance_plans',
    columns: ['record_type_code','record_category_code','reinsurance_year','plan_code','plan_name','plan_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'plan_code',
  },
  A00510: {
    table: 'practice_types',
    columns: ['record_type_code','record_category_code','reinsurance_year','commodity_code','practice_code','practice_name','practice_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'commodity_code,practice_code',
  },
  A00540: {
    table: 'type_codes',
    columns: ['record_type_code','record_category_code','reinsurance_year','commodity_code','type_code','type_name','type_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'commodity_code,type_code',
  },
  A00410: {
    table: 'classes',
    columns: ['record_type_code','record_category_code','reinsurance_year','class_code','class_name','class_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'class_code',
  },
  A00430: {
    table: 'commodity_types',
    columns: ['record_type_code','record_category_code','reinsurance_year','commodity_type_code','commodity_type_name','commodity_type_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'commodity_type_code',
  },
  A00470: {
    table: 'intended_uses',
    columns: ['record_type_code','record_category_code','reinsurance_year','intended_use_code','intended_use_name','intended_use_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'intended_use_code',
  },
  A00490: {
    table: 'irrigation_practices',
    columns: ['record_type_code','record_category_code','reinsurance_year','irrigation_practice_code','irrigation_practice_name','irrigation_practice_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'irrigation_practice_code',
  },
  A00450: {
    table: 'cropping_practices',
    columns: ['record_type_code','record_category_code','reinsurance_year','cropping_practice_code','cropping_practice_name','cropping_practice_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'cropping_practice_code',
  },
  A00500: {
    table: 'organic_practices',
    columns: ['record_type_code','record_category_code','reinsurance_year','organic_practice_code','organic_practice_name','organic_practice_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'organic_practice_code',
  },
  A00480: {
    table: 'intervals',
    columns: ['record_type_code','record_category_code','reinsurance_year','interval_code','interval_name','interval_abbreviation','interval_start_date','interval_end_date','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'reinsurance_year,interval_code',
  },
  A00530: {
    table: 'sub_classes',
    columns: ['record_type_code','record_category_code','reinsurance_year','sub_class_code','sub_class_name','sub_class_abbreviation','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year']),
    conflictColumns: 'reinsurance_year,sub_class_code',
  },
  A00070: {
    table: 'subsidy_percents',
    columns: ['record_type_code','record_category_code','reinsurance_year','commodity_code','unit_structure_code','insurance_plan_code','coverage_level_percent','coverage_type_code','deductible_amount','endorsement_length_code','endorsement_length_count','insurance_option_code','range_type_code','range_low_value','range_high_value','subsidy_percent','last_released_date','released_date','deleted_date'],
    numericColumns: makeNumericSet(['reinsurance_year','coverage_level_percent','deductible_amount','endorsement_length_count','range_low_value','range_high_value','subsidy_percent']),
    conflictColumns: 'reinsurance_year,commodity_code,unit_structure_code,insurance_plan_code,coverage_level_percent,coverage_type_code,insurance_option_code',
  },

  // Core data tables
  A00030: {
    table: 'insurance_offers',
    columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','type_code','practice_code','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','unit_of_measure_abbreviation','program_type_code','beta_id','quality_id','unit_discount_id','historical_yield_trend_id','draw_id','optional_unit_allowed_flag','basic_unit_allowed_flag','enterprise_unit_allowed_flag','whole_farm_unit_allowed_flag','type_practice_use_code','private_508h_flag','hip_rate_id','pace_date_id','pace_rate_id','last_released_date','released_date','deleted_date','filing_date'],
    numericColumns: makeNumericSet(['reinsurance_year','commodity_year']),
    conflictColumns: 'adm_insurance_offer_id',
  },
  A00810: {
    table: 'adm_prices',
    columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','crush_district_number','type_code','practice_code','insurance_option_code','range_class_code','coverage_level_percent','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','catastrophic_price','established_price','additional_price','season_average_price','contract_price_code','maximum_contract_price','maximum_over_price_election','maximum_contract_price_factor','county_base_value','projected_price','harvest_price','price_volatility_factor','allowable_cost_price','maximum_protection_per_acre','catastrophic_dollar_amount','reference_maximum_dollar_amount','density_low_quantity','density_high_quantity','age_low_count','age_high_count','minimum_dollar_amount','maximum_dollar_amount','harvest_revenue_option_factor','sucrose_factor','survival_percent','minimum_acre_percent','maximum_acre_percent','additional_value_price','maximum_additional_value_price','certified_seed_price','hybrid_seed_option_price','fixed_coverage_amount','growth_stage_code','growth_stage_factor','expected_revenue_factor','expected_county_landing_adjustment_factor','minimum_value_price','harvest_cost_amount','post_production_cost_amount','fresh_fruit_factor','expected_margin_amount','final_margin_amount','expected_index_value','final_index_value','expected_revenue_amount','final_revenue_amount','average_index_value','maximum_over_established_price','harvest_cost_amount_hand','harvest_cost_amount_machine','harvest_price_released_date','base_weight','projected_price_adjustment_factor','harvest_price_adjustment_factor','price_factor','last_released_date','released_date','deleted_date','filing_date'],
    numericColumns: makeNumericSet(['reinsurance_year','commodity_year','catastrophic_price','established_price','additional_price','season_average_price','maximum_contract_price','maximum_over_price_election','maximum_contract_price_factor','county_base_value','projected_price','harvest_price','price_volatility_factor','allowable_cost_price','maximum_protection_per_acre','catastrophic_dollar_amount','reference_maximum_dollar_amount','density_low_quantity','density_high_quantity','age_low_count','age_high_count','minimum_dollar_amount','maximum_dollar_amount','harvest_revenue_option_factor','sucrose_factor','survival_percent','minimum_acre_percent','maximum_acre_percent','additional_value_price','maximum_additional_value_price','certified_seed_price','hybrid_seed_option_price','fixed_coverage_amount','growth_stage_factor','expected_revenue_factor','expected_county_landing_adjustment_factor','minimum_value_price','harvest_cost_amount','post_production_cost_amount','fresh_fruit_factor','expected_margin_amount','final_margin_amount','expected_index_value','final_index_value','expected_revenue_amount','final_revenue_amount','average_index_value','maximum_over_established_price','harvest_cost_amount_hand','harvest_cost_amount_machine','base_weight','projected_price_adjustment_factor','harvest_price_adjustment_factor','price_factor']),
    conflictColumns: 'adm_insurance_offer_id,released_date',
  },
  A00200: {
    table: 'adm_dates',
    columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','type_code','practice_code','insurance_option_code','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','contract_change_date','sales_closing_date','modified_sales_closing_date','extended_sales_closing_date','earliest_planting_date','final_planting_date','extended_final_planting_date','acreage_reporting_date','modified_acreage_reporting_date','end_of_insurance_date','cancellation_date','modified_cancellation_date','termination_date','premium_billing_date','production_reporting_date','modified_production_reporting_date','end_of_late_planting_period_date','sales_period_begin_date','sales_period_end_date','insurance_attachment_date','commodity_reporting_date','modified_commodity_reporting_date','insured_production_reporting_date','modified_insured_production_reporting_date','last_released_date','released_date','deleted_date','filing_date'],
    numericColumns: makeNumericSet(['reinsurance_year','commodity_year']),
    conflictColumns: 'adm_insurance_offer_id,released_date',
  },
  A01010: {
    table: 'base_rates',
    columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','type_code','practice_code','range_class_code','coverage_level_percent','wa_number','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','county_yield','density_low_quantity','density_high_quantity','reference_amount','reference_amount_code','reference_rate','exponent_value','fixed_rate','prior_year_reference_amount','prior_year_reference_rate','prior_year_exponent_value','prior_year_fixed_rate','base_rate','prior_year_base_rate','last_released_date','released_date','deleted_date','filing_date'],
    numericColumns: makeNumericSet(['reinsurance_year','commodity_year','coverage_level_percent','county_yield','density_low_quantity','density_high_quantity','reference_amount','reference_rate','exponent_value','fixed_rate','prior_year_reference_amount','prior_year_reference_rate','prior_year_exponent_value','prior_year_fixed_rate','base_rate','prior_year_base_rate']),
    conflictColumns: 'adm_insurance_offer_id,released_date',
  },
  A01100: {
    table: 'yields',
    columns: ['record_type_code','record_category_code','adm_insurance_offer_id','reinsurance_year','commodity_year','commodity_code','insurance_plan_code','state_code','county_code','sub_county_code','type_code','practice_code','transitional_amount_code','leaf_year','characteristic_code','density_low_quantity','density_high_quantity','prior_commodity_year','wa_number','wa_land_id','commodity_type_code','class_code','sub_class_code','intended_use_code','irrigation_practice_code','cropping_practice_code','organic_practice_code','interval_code','transitional_amount','prior_transitional_amount','characteristic_name','last_reported_leaf_year','transitional_amount_uom_abbreviation','last_released_date','released_date','deleted_date','filing_date'],
    numericColumns: makeNumericSet(['reinsurance_year','commodity_year','density_low_quantity','density_high_quantity','transitional_amount','prior_transitional_amount']),
    conflictColumns: 'adm_insurance_offer_id,transitional_amount_code,leaf_year,released_date',
  },
};

// Map filename patterns to record type codes
function detectRecordType(filename: string): string | null {
  // Filenames like: 2026_A00810_Price_YTD.txt or 2026_A00420_Commodity_Daily.txt
  const match = filename.match(/A(\d{5})/);
  if (!match) return null;
  return `A${match[1]}`;
}

// ============================================================
// Parsing
// ============================================================

function parsePipeDelimited(
  content: string,
  config: RecordConfig,
  sourceFile: string
): Record<string, unknown>[] {
  const lines = content.split('\n');
  const rows: Record<string, unknown>[] = [];

  // Skip header line
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = line.split('|');
    const row: Record<string, unknown> = {};

    // Only add source_file for tables that have the column
    if (config.table === 'insurance_offers' || config.table === 'adm_prices' ||
        config.table === 'adm_dates' || config.table === 'base_rates' ||
        config.table === 'yields') {
      row.source_file = sourceFile;
    }

    for (let j = 0; j < config.columns.length && j < values.length; j++) {
      const col = config.columns[j];
      const val = values[j]?.trim() ?? '';

      // Skip record_type_code and record_category_code for lookup tables
      // that don't have those columns in the DB
      if (col === 'record_type_code' || col === 'record_category_code' ||
          col === 'last_released_date' || col === 'released_date' || col === 'deleted_date') {
        // Only include released_date/deleted_date for core data tables
        if (col === 'released_date' || col === 'deleted_date') {
          if (config.table === 'insurance_offers' || config.table === 'adm_prices' ||
              config.table === 'adm_dates' || config.table === 'base_rates' ||
              config.table === 'yields') {
            row[col] = val || null;
          }
        }
        // Skip these columns for lookup tables
        if (!['insurance_offers','adm_prices','adm_dates','base_rates','yields','subsidy_percents'].includes(config.table)) {
          continue;
        }
        if (col === 'record_type_code' || col === 'record_category_code' ||
            col === 'last_released_date') {
          continue;
        }
      }

      if (val === '') {
        row[col] = null;
      } else if (config.numericColumns.has(col)) {
        const num = parseFloat(val);
        row[col] = isNaN(num) ? null : num;
      } else {
        row[col] = val;
      }
    }

    rows.push(row);
  }

  return rows;
}

async function upsertBatch(
  table: string,
  rows: Record<string, unknown>[],
  conflictColumns: string
): Promise<{ upserted: number; errors: string[] }> {
  let upserted = 0;
  const errors: string[] = [];

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const { error } = await supabase
      .from(table)
      .upsert(batch, { onConflict: conflictColumns });

    if (error) {
      errors.push(`Batch ${Math.floor(i / BATCH_SIZE)}: ${error.message}`);
    } else {
      upserted += batch.length;
    }
  }

  return { upserted, errors };
}

// ============================================================
// Main entry points
// ============================================================

interface ParseResult {
  recordType: string;
  table: string;
  rowsProcessed: number;
  rowsUpserted: number;
  errors: string[];
}

/**
 * Parse a single .txt file and load into the appropriate table.
 */
async function parseTextFile(
  content: string,
  filename: string,
  sourceFile: string
): Promise<ParseResult | null> {
  const recordType = detectRecordType(filename);
  if (!recordType) return null;

  const config = RECORD_CONFIGS[recordType];
  if (!config) return null; // Unsupported record type, skip

  const rows = parsePipeDelimited(content, config, sourceFile);
  if (rows.length === 0) {
    return { recordType, table: config.table, rowsProcessed: 0, rowsUpserted: 0, errors: [] };
  }

  const { upserted, errors } = await upsertBatch(config.table, rows, config.conflictColumns);

  return {
    recordType,
    table: config.table,
    rowsProcessed: rows.length,
    rowsUpserted: upserted,
    errors,
  };
}

/**
 * Parse and load a downloaded ZIP file into the database.
 */
export async function parseAndLoadFile(entry: ManifestEntry): Promise<{
  recordTypes: ParseResult[];
  totalProcessed: number;
  totalUpserted: number;
  errors: string[];
}> {
  const zipPath = path.join(DATA_DIR, entry.localPath);

  if (!fs.existsSync(zipPath)) {
    return {
      recordTypes: [],
      totalProcessed: 0,
      totalUpserted: 0,
      errors: [`File not found: ${zipPath}`],
    };
  }

  const zip = new AdmZip(zipPath);
  const zipEntries = zip.getEntries();
  const results: ParseResult[] = [];
  const allErrors: string[] = [];
  let totalProcessed = 0;
  let totalUpserted = 0;

  // Process lookup tables first, then core data
  const lookupCodes = ['A00520','A00440','A00420','A00460','A00510','A00540','A00410','A00430','A00470','A00480','A00490','A00450','A00500','A00530','A00070'];
  const coreCodes = ['A00030','A00810','A00200','A01010','A01100'];

  const orderedEntries = [...zipEntries].sort((a, b) => {
    const aCode = detectRecordType(a.entryName) || '';
    const bCode = detectRecordType(b.entryName) || '';
    const aIsLookup = lookupCodes.includes(aCode);
    const bIsLookup = lookupCodes.includes(bCode);
    if (aIsLookup && !bIsLookup) return -1;
    if (!aIsLookup && bIsLookup) return 1;
    return 0;
  });

  for (const zipEntry of orderedEntries) {
    const name = zipEntry.entryName;
    if (!name.endsWith('.txt') && !name.endsWith('.TXT')) continue;

    const recordType = detectRecordType(name);
    if (!recordType || !RECORD_CONFIGS[recordType]) continue;

    const content = zipEntry.getData().toString('utf-8');
    if (!content.trim()) continue;

    try {
      const result = await parseTextFile(content, name, entry.filename);
      if (result) {
        results.push(result);
        totalProcessed += result.rowsProcessed;
        totalUpserted += result.rowsUpserted;
        allErrors.push(...result.errors);
      }
    } catch (err) {
      allErrors.push(`Error parsing ${name}: ${(err as Error).message}`);
    }
  }

  // Log the ingestion
  await supabase.from('ingestion_log').insert({
    source: entry.source,
    filename: entry.filename,
    record_type: results.map(r => r.recordType).join(','),
    rows_processed: totalProcessed,
    rows_upserted: totalUpserted,
    status: allErrors.length > 0 ? 'partial' : 'success',
    error_message: allErrors.length > 0 ? allErrors.join('; ') : null,
    completed_at: new Date().toISOString(),
  });

  // Mark as parsed in manifest
  const manifest = readManifest();
  const key = manifestKey(entry.source, entry.filename);
  if (manifest.files[key]) {
    manifest.files[key].parsed = true;
    manifest.files[key].parsedAt = new Date().toISOString();
    writeManifest(manifest);
  }

  return { recordTypes: results, totalProcessed, totalUpserted, errors: allErrors };
}

// ============================================================
// Checkpoint support for resumable loads
// ============================================================

const CHECKPOINT_DIR = path.join(DATA_DIR, '.checkpoints');

function getCheckpointPath(filePath: string): string {
  const key = path.basename(filePath).replace(/[^a-zA-Z0-9_.-]/g, '_');
  return path.join(CHECKPOINT_DIR, `${key}.json`);
}

function readCheckpoint(filePath: string): { lineNumber: number; rowsUpserted: number } | null {
  const cpPath = getCheckpointPath(filePath);
  if (!fs.existsSync(cpPath)) return null;
  try {
    const data = JSON.parse(fs.readFileSync(cpPath, 'utf-8'));
    return { lineNumber: data.lineNumber || 0, rowsUpserted: data.rowsUpserted || 0 };
  } catch {
    return null;
  }
}

function writeCheckpoint(filePath: string, lineNumber: number, rowsUpserted: number): void {
  if (!fs.existsSync(CHECKPOINT_DIR)) {
    fs.mkdirSync(CHECKPOINT_DIR, { recursive: true });
  }
  const cpPath = getCheckpointPath(filePath);
  fs.writeFileSync(cpPath, JSON.stringify({ lineNumber, rowsUpserted, updatedAt: new Date().toISOString() }));
}

function clearCheckpoint(filePath: string): void {
  const cpPath = getCheckpointPath(filePath);
  if (fs.existsSync(cpPath)) fs.unlinkSync(cpPath);
}

/**
 * Parse a raw .txt file using streaming — reads line by line,
 * batches rows, and upserts as it goes. Handles multi-million row files
 * without loading everything into memory.
 *
 * Supports resumable loads via checkpoints — if the server crashes mid-load,
 * re-running the same file will skip already-processed lines.
 */
export async function parseRawTextFile(filePath: string, sourceLabel: string, onProgress?: (processed: number, upserted: number) => void): Promise<ParseResult | null> {
  const filename = path.basename(filePath);
  const recordType = detectRecordType(filename);
  if (!recordType) return null;

  const config = RECORD_CONFIGS[recordType];
  if (!config) return null;

  // Check for existing checkpoint to resume from
  const checkpoint = readCheckpoint(filePath);
  const skipToLine = checkpoint?.lineNumber ?? 0;
  let resumedUpserted = checkpoint?.rowsUpserted ?? 0;

  const readline = await import('readline');
  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let currentLine = 0;
  let rowsProcessed = skipToLine; // Start count from where we left off
  let rowsUpserted = resumedUpserted;
  const errors: string[] = [];
  let batch: Record<string, unknown>[] = [];
  const STREAM_BATCH_SIZE = 2000;

  if (skipToLine > 0) {
    onProgress?.(rowsProcessed, rowsUpserted);
  }

  for await (const line of rl) {
    currentLine++;

    // Skip header (line 1)
    if (currentLine === 1) continue;

    // Skip lines we already processed in a previous run
    if (currentLine <= skipToLine) continue;

    const trimmed = line.trim();
    if (!trimmed) continue;

    const values = trimmed.split('|');
    const row: Record<string, unknown> = {};

    // Add source_file for core data tables
    if (['insurance_offers','adm_prices','adm_dates','base_rates','yields'].includes(config.table)) {
      row.source_file = sourceLabel;
    }

    for (let j = 0; j < config.columns.length && j < values.length; j++) {
      const col = config.columns[j];
      const val = values[j]?.trim() ?? '';

      // Skip columns not in the DB for lookup tables
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
        row[col] = null;
      } else if (config.numericColumns.has(col)) {
        const num = parseFloat(val);
        row[col] = isNaN(num) ? null : num;
      } else {
        row[col] = val;
      }
    }

    batch.push(row);
    rowsProcessed++;

    // Flush batch when full
    if (batch.length >= STREAM_BATCH_SIZE) {
      const { upserted, errors: batchErrors } = await upsertBatch(config.table, batch, config.conflictColumns);
      rowsUpserted += upserted;
      errors.push(...batchErrors);
      batch = [];

      // Save checkpoint every batch
      writeCheckpoint(filePath, currentLine, rowsUpserted);
      onProgress?.(rowsProcessed, rowsUpserted);
    }
  }

  // Flush remaining rows
  if (batch.length > 0) {
    const { upserted, errors: batchErrors } = await upsertBatch(config.table, batch, config.conflictColumns);
    rowsUpserted += upserted;
    errors.push(...batchErrors);
  }

  // Clear checkpoint on successful completion
  clearCheckpoint(filePath);

  return {
    recordType,
    table: config.table,
    rowsProcessed,
    rowsUpserted,
    errors,
  };
}
