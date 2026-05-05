// Self-contained tests for loader.mjs RECORD_CONFIGS + parseRow on the
// Phase 13A record types (A01040, A01090). Validates that one real row
// from each file lands in the expected columns with the expected types.
//
// CarrackRMA has no test runner (per Phase 4.6 brief: not adding test
// infra this round). Run directly: `node scripts/test_rma_parser.mjs`.
// Exits 0 on pass, 1 on fail.

import { RECORD_CONFIGS, parseRow } from '../loader.mjs';

let passed = 0;
let failed = 0;
const failures = [];

/** @param {string} name @param {() => void} fn */
function test(name, fn) {
  try {
    fn();
    passed++;
    console.log(`  ok   ${name}`);
  } catch (err) {
    failed++;
    failures.push({ name, err });
    console.log(`  FAIL ${name}`);
  }
}

/** @param {unknown} actual @param {unknown} expected @param {string} [msg] */
function assertEqual(actual, expected, msg) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) {
    throw new Error(`${msg ? msg + ' — ' : ''}expected ${e}, got ${a}`);
  }
}

/** @param {boolean} cond @param {string} msg */
function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

console.log('\n  RECORD_CONFIGS registration');

test('A01040 is registered as coverage_level_differential', () => {
  const cfg = RECORD_CONFIGS.A01040;
  assert(!!cfg, 'A01040 missing from RECORD_CONFIGS');
  assertEqual(cfg.table, 'coverage_level_differential');
  assertEqual(
    cfg.conflictColumns,
    'adm_insurance_offer_id,coverage_level_percent,coverage_type_code,released_date',
  );
});

test('A01090 is registered as unit_discount', () => {
  const cfg = RECORD_CONFIGS.A01090;
  assert(!!cfg, 'A01090 missing from RECORD_CONFIGS');
  assertEqual(cfg.table, 'unit_discount');
  assertEqual(
    cfg.conflictColumns,
    'reinsurance_year,unit_discount_id,coverage_level_percent,area_low_quantity,area_high_quantity',
  );
});

console.log('\n  parseRow: A01040 (CoverageLevelDifferential)');

// Real row from 2026_A01040_CoverageLevelDifferential_YTD.txt (line 2).
// Header order: record_type|record_category|adm_insurance_offer_id|reinsurance_year|...
const A01040_ROW =
  'A01040|05|37991959|2026|2026|0081|03|05|069|CCC|091|728||0.70|A|||091|997|997|997|003|120|002|997|1.023600000|1.0049|1.0025|1.0000|1.056200000|1.0049|1.0025|1.0000||||20251120||20251130';

test('A01040 row parses key offer + coverage fields', () => {
  const cfg = RECORD_CONFIGS.A01040;
  const row = parseRow(A01040_ROW.split('|'), cfg, 'test_A01040.txt');
  assertEqual(row.adm_insurance_offer_id, '37991959');
  assertEqual(row.reinsurance_year, 2026);
  assertEqual(row.commodity_year, 2026);
  assertEqual(row.commodity_code, '0081');
  assertEqual(row.insurance_plan_code, '03');
  assertEqual(row.state_code, '05');
  assertEqual(row.county_code, '069');
  assertEqual(row.sub_county_code, 'CCC');
  assertEqual(row.coverage_level_percent, 0.7);
  assertEqual(row.coverage_type_code, 'A');
});

test('A01040 row parses rate/residual factors as numbers', () => {
  const cfg = RECORD_CONFIGS.A01040;
  const row = parseRow(A01040_ROW.split('|'), cfg, 'test_A01040.txt');
  assertEqual(row.rate_differential_factor, 1.0236);
  assertEqual(row.unit_residual_factor, 1.0049);
  assertEqual(row.enterprise_unit_residual_factor, 1.0025);
  assertEqual(row.whole_farm_unit_residual_factor, 1);
  assertEqual(row.prior_year_rate_differential_factor, 1.0562);
  // CAT residual factors are empty in this row -> null
  assertEqual(row.cat_residual_factor, null);
  assertEqual(row.prior_cat_residual_factor, null);
});

test('A01040 row preserves provenance (released_date, filing_date, source_file)', () => {
  const cfg = RECORD_CONFIGS.A01040;
  const row = parseRow(A01040_ROW.split('|'), cfg, 'test_A01040.txt');
  assertEqual(row.released_date, '20251120');
  assertEqual(row.deleted_date, null);
  assertEqual(row.filing_date, '20251130');
  assertEqual(row.source_file, 'test_A01040.txt');
});

test('A01040 row drops record_type_code / record_category_code / last_released_date', () => {
  const cfg = RECORD_CONFIGS.A01040;
  const row = parseRow(A01040_ROW.split('|'), cfg, 'test_A01040.txt');
  assert(!('record_type_code' in row), 'record_type_code should be skipped');
  assert(!('record_category_code' in row), 'record_category_code should be skipped');
  assert(!('last_released_date' in row), 'last_released_date should be skipped');
});

console.log('\n  parseRow: A01090 (UnitDiscount)');

// Real rows from 2026_A01090_UnitDiscount_YTD.txt.
// Header: record_type|record_category|reinsurance_year|unit_discount_id|coverage_level_percent|area_low_qty|area_high_qty|...
const A01090_SIMPLE_ROW =
  'A01090|02|2026|1840001||||||||||||||||||1.000|0.900|0.900|Acres||20260122|';
const A01090_BANDED_ROW =
  'A01090|04|2026|1020201|0.55|400.00|799.99|||||||||||||||1.000|0.701|0.701|Acres||20250604|';

test('A01090 simple (category-02) row parses with null CLP/area bounds', () => {
  const cfg = RECORD_CONFIGS.A01090;
  const row = parseRow(A01090_SIMPLE_ROW.split('|'), cfg, 'test_A01090.txt');
  assertEqual(row.reinsurance_year, 2026);
  assertEqual(row.unit_discount_id, '1840001');
  assertEqual(row.coverage_level_percent, null);
  assertEqual(row.area_low_quantity, null);
  assertEqual(row.area_high_quantity, null);
  assertEqual(row.optional_unit_discount_factor, 1);
  assertEqual(row.basic_unit_discount_factor, 0.9);
  assertEqual(row.enterprise_unit_discount_factor, 0.9);
  assertEqual(row.area_description, 'Acres');
});

test('A01090 banded (category-04) row parses CLP + area bounds + factors', () => {
  const cfg = RECORD_CONFIGS.A01090;
  const row = parseRow(A01090_BANDED_ROW.split('|'), cfg, 'test_A01090.txt');
  assertEqual(row.reinsurance_year, 2026);
  assertEqual(row.unit_discount_id, '1020201');
  assertEqual(row.coverage_level_percent, 0.55);
  assertEqual(row.area_low_quantity, 400);
  assertEqual(row.area_high_quantity, 799.99);
  assertEqual(row.optional_unit_discount_factor, 1);
  assertEqual(row.basic_unit_discount_factor, 0.701);
  assertEqual(row.enterprise_unit_discount_factor, 0.701);
});

test('A01090 row excludes lookup-only metadata (no source_file / released_date)', () => {
  const cfg = RECORD_CONFIGS.A01090;
  const row = parseRow(A01090_BANDED_ROW.split('|'), cfg, 'test_A01090.txt');
  assert(!('source_file' in row), 'unit_discount is lookup-style; source_file should not be set');
  assert(!('released_date' in row), 'unit_discount is lookup-style; released_date should not be set');
  assert(!('deleted_date' in row), 'unit_discount is lookup-style; deleted_date should not be set');
  assert(!('record_type_code' in row));
  assert(!('record_category_code' in row));
  assert(!('last_released_date' in row));
});

console.log(`\n  ${passed} passed, ${failed} failed\n`);

if (failed > 0) {
  console.log('  Failures:');
  for (const f of failures) {
    console.log(`    ${f.name}: ${f.err.message}`);
  }
  process.exit(1);
}
