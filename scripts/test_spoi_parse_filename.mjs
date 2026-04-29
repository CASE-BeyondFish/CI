// Self-contained tests for scripts/spoi_parse_filename.mjs.
// CarrackRMA has no test runner (per Phase 4.6 brief: not adding test
// infra this round). Run directly: `node scripts/test_spoi_parse_filename.mjs`.
// Exits 0 on pass, 1 on fail.

import { parseSpoiPath, naturalKey, storagePath } from './spoi_parse_filename.mjs';

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

console.log('parseSpoiPath');

test('the two discovery-report sample paths parse correctly', () => {
  // From the discovery report, both should parse cleanly.
  const a = parseSpoiPath(
    'data/special_provisions/2026/20251130/01/0011/02_170_01_0011_20251130.pdf'
  );
  assertEqual(a, {
    year: 2026,
    state_code: '02',
    county_code: '170',
    insurance_plan_code: '01',
    commodity_code: '0011',
    filing_date: '2025-11-30',
  });

  const b = parseSpoiPath(
    'data/special_provisions/2026/20251231/90/0086/01_115_90_0086_20251231.pdf'
  );
  assertEqual(b, {
    year: 2026,
    state_code: '01',
    county_code: '115',
    insurance_plan_code: '90',
    commodity_code: '0086',
    filing_date: '2025-12-31',
  });
});

test('absolute Windows path with backslashes', () => {
  const r = parseSpoiPath(
    'R:\\CarrackRMA\\data\\special_provisions\\2026\\20251130\\01\\0011\\02_170_01_0011_20251130.pdf'
  );
  assertEqual(r?.year, 2026);
  assertEqual(r?.state_code, '02');
  assertEqual(r?.filing_date, '2025-11-30');
});

test('absolute POSIX path with leading prefix', () => {
  const r = parseSpoiPath(
    '/some/other/root/data/special_provisions/2027/20260228/04/0115/05_003_04_0115_20260228.pdf'
  );
  assertEqual(r?.year, 2027);
  assertEqual(r?.commodity_code, '0115');
});

test('non-pdf file returns null', () => {
  assertEqual(
    parseSpoiPath('data/special_provisions/2026/20251130/01/0011/02_170_01_0011_20251130.txt'),
    null
  );
});

test('path missing the special_provisions root returns null', () => {
  assertEqual(
    parseSpoiPath('data/adm/2026/20251130/01/0011/02_170_01_0011_20251130.pdf'),
    null
  );
});

test('filename plan disagreeing with directory plan returns null', () => {
  // dir says plan 01, filename says plan 02 — reject
  assertEqual(
    parseSpoiPath('data/special_provisions/2026/20251130/01/0011/02_170_02_0011_20251130.pdf'),
    null
  );
});

test('filename commodity disagreeing with directory commodity returns null', () => {
  assertEqual(
    parseSpoiPath('data/special_provisions/2026/20251130/01/0011/02_170_01_0099_20251130.pdf'),
    null
  );
});

test('filename filing disagreeing with directory filing returns null', () => {
  assertEqual(
    parseSpoiPath('data/special_provisions/2026/20251130/01/0011/02_170_01_0011_20251231.pdf'),
    null
  );
});

test('impossible date (2025-13-01) returns null', () => {
  assertEqual(
    parseSpoiPath('data/special_provisions/2026/20251301/01/0011/02_170_01_0011_20251301.pdf'),
    null
  );
});

test('empty / non-string input returns null', () => {
  assertEqual(parseSpoiPath(''), null);
  // @ts-expect-error testing runtime guard
  assertEqual(parseSpoiPath(null), null);
  // @ts-expect-error testing runtime guard
  assertEqual(parseSpoiPath(undefined), null);
});

console.log('\nnaturalKey');

test('produces stable, distinct keys', () => {
  const a = naturalKey({ year: 2026, state_code: '02', county_code: '170', insurance_plan_code: '01', commodity_code: '0011' });
  const b = naturalKey({ year: 2026, state_code: '01', county_code: '115', insurance_plan_code: '90', commodity_code: '0086' });
  assertEqual(a, '2026|02|170|01|0011');
  assertEqual(b, '2026|01|115|90|0086');
  if (a === b) throw new Error('different inputs produced same key');
});

console.log('\nstoragePath');

test('omits filing_date so latest-filing overwrites', () => {
  const p = storagePath({ year: 2026, state_code: '02', county_code: '170', insurance_plan_code: '01', commodity_code: '0011' });
  assertEqual(p, '2026/02_170_01_0011.pdf');
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  for (const f of failures) {
    console.error(`  ${f.name}:\n    ${f.err.message}`);
  }
  process.exit(1);
}
