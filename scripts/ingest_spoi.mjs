/**
 * One-shot ingest of Special Provisions PDFs.
 *
 * Walks data/special_provisions/ recursively, dedups to "latest filing
 * per (year, state, county, plan, commodity)", uploads each survivor's
 * PDF to the spoi-documents Supabase Storage bucket, and upserts a row
 * in public.spoi_documents.
 *
 * Usage:
 *   node scripts/ingest_spoi.mjs                       # all years, real run, 4 parallel
 *   node scripts/ingest_spoi.mjs --dry-run             # walk + dedup + summary, no writes
 *   node scripts/ingest_spoi.mjs --year=2026           # restrict to one year
 *   node scripts/ingest_spoi.mjs --concurrency=8       # bump parallelism (max 16)
 *   node scripts/ingest_spoi.mjs --year=2026 --dry-run --concurrency=8
 *
 * Env (loaded from .env.local at the project root):
 *   SUPABASE_URL          required for non-dry-run
 *   SUPABASE_SERVICE_KEY  required for non-dry-run (writes need service role)
 *
 * The script is intentionally scrappy — one-shot tool, not infra.
 * Re-running is safe: storage uploads use upsert, DB writes upsert on
 * the natural key.
 */

import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { parseSpoiPath, storagePath } from './spoi_parse_filename.mjs';

dotenv.config({ path: '.env.local' });

const DATA_ROOT = path.resolve('data/special_provisions');
const STORAGE_BUCKET = 'spoi-documents';
const CATALOG_TABLE = 'spoi_documents';

// ---------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const yearArg = args.find((a) => a.startsWith('--year='));
const yearFilter = yearArg ? Number(yearArg.split('=')[1]) : null;
const concurrencyArg = args.find((a) => a.startsWith('--concurrency='));
const concurrency = concurrencyArg ? Number(concurrencyArg.split('=')[1]) : 4;

if (yearArg && (!Number.isInteger(yearFilter) || yearFilter < 1900 || yearFilter > 2100)) {
  console.error(`bad --year value: ${yearArg}`);
  process.exit(2);
}
if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 16) {
  console.error(`bad --concurrency value (1-16): ${concurrencyArg ?? concurrency}`);
  process.exit(2);
}

// ---------------------------------------------------------------
// Walk + parse + dedup
// ---------------------------------------------------------------

/** Recursively yield every .pdf path under root. */
function* walkPdfs(root) {
  const stack = [root];
  while (stack.length > 0) {
    const dir = stack.pop();
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        stack.push(full);
      } else if (e.isFile() && e.name.toLowerCase().endsWith('.pdf')) {
        yield full;
      }
    }
  }
}

/**
 * Walk the SPOI tree, parse every PDF, group by natural key and keep
 * only the latest filing per group. Returns the survivor records plus
 * counters for the summary.
 *
 * @param {number | null} yearFilterArg
 */
function discoverSurvivors(yearFilterArg) {
  let scanned = 0;
  let parseFailed = 0;
  let yearSkipped = 0;
  /** @type {Map<string, {parsed: import('./spoi_parse_filename.mjs').ParsedSpoi, absPath: string, sizeBytes: number}>} */
  const winners = new Map();

  if (!fs.existsSync(DATA_ROOT)) {
    console.error(`data root does not exist: ${DATA_ROOT}`);
    process.exit(2);
  }

  for (const absPath of walkPdfs(DATA_ROOT)) {
    scanned++;
    const parsed = parseSpoiPath(absPath);
    if (!parsed) {
      parseFailed++;
      if (parseFailed <= 5) console.warn(`  skip (unparseable): ${absPath}`);
      continue;
    }
    if (yearFilterArg !== null && parsed.year !== yearFilterArg) {
      yearSkipped++;
      continue;
    }
    const key = `${parsed.year}|${parsed.state_code}|${parsed.county_code}|${parsed.insurance_plan_code}|${parsed.commodity_code}`;
    const existing = winners.get(key);
    if (!existing || parsed.filing_date > existing.parsed.filing_date) {
      let sizeBytes = 0;
      try {
        sizeBytes = fs.statSync(absPath).size;
      } catch { /* leave 0 */ }
      winners.set(key, { parsed, absPath, sizeBytes });
    }
  }

  return { scanned, parseFailed, yearSkipped, winners };
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------

async function main() {
  const startedAt = Date.now();
  console.log(`SPOI ingest — ${dryRun ? 'DRY RUN' : 'LIVE'}${yearFilter ? ` (year=${yearFilter})` : ''}`);
  console.log(`  data root: ${DATA_ROOT}`);

  const { scanned, parseFailed, yearSkipped, winners } = discoverSurvivors(yearFilter);

  // Per-year breakdown of survivors
  const byYear = new Map();
  for (const w of winners.values()) {
    byYear.set(w.parsed.year, (byYear.get(w.parsed.year) ?? 0) + 1);
  }
  const yearsSorted = [...byYear.keys()].sort((a, b) => a - b);

  console.log(`\nDiscovery summary`);
  console.log(`  scanned files:       ${scanned}`);
  console.log(`  parse failures:      ${parseFailed}${parseFailed > 5 ? ' (first 5 logged above)' : ''}`);
  if (yearFilter !== null) {
    console.log(`  filtered out by --year=${yearFilter}: ${yearSkipped}`);
  }
  console.log(`  unique tuples (latest filing wins): ${winners.size}`);
  for (const y of yearsSorted) {
    console.log(`    ${y}: ${byYear.get(y)}`);
  }

  if (dryRun) {
    console.log(`\nDry run — no uploads, no DB writes. Done in ${((Date.now() - startedAt) / 1000).toFixed(1)}s.`);
    return;
  }

  // ---------------------------------------------------------------
  // Live run — pooled concurrent ingest.
  // ---------------------------------------------------------------

  const supabase = makeSupabaseClient();
  console.log(
    `\nLive run — uploading to bucket "${STORAGE_BUCKET}", upserting into ${CATALOG_TABLE}` +
    ` (concurrency=${concurrency})...`
  );

  const items = [...winners.values()];
  const total = items.length;
  let uploaded = 0;
  let failed = 0;
  /** @type {{path: string, error: string}[]} */
  const failures = [];
  let lastLoggedAt = Date.now();

  await runPool(items, concurrency, async (w) => {
    const result = await ingestOne(supabase, w);
    if (result.ok) {
      uploaded++;
    } else {
      failed++;
      failures.push({ path: w.absPath, error: result.error });
    }
    const done = uploaded + failed;
    // Log every 100 completions OR every 10s, whichever comes first.
    const now = Date.now();
    if (done % 100 === 0 || done === total || now - lastLoggedAt > 10000) {
      lastLoggedAt = now;
      const elapsed = ((now - startedAt) / 1000).toFixed(0);
      const rate = (done / Math.max(1, (now - startedAt) / 1000)).toFixed(1);
      console.log(`  [${done}/${total}] uploaded=${uploaded} failed=${failed} elapsed=${elapsed}s rate=${rate}/s`);
    }
  });

  console.log(`\nDone in ${((Date.now() - startedAt) / 1000).toFixed(1)}s. ingested=${uploaded} failed=${failed}`);
  if (failures.length > 0) {
    console.log(`\nFailures (first 20):`);
    for (const f of failures.slice(0, 20)) {
      console.log(`  ${f.path}\n    ${f.error}`);
    }
    if (failures.length > 20) console.log(`  ... and ${failures.length - 20} more`);
    process.exit(1);
  }
}

/**
 * Process `items` with `concurrency` parallel workers. Each worker is
 * an async loop pulling indices off a shared counter — no extra deps,
 * no library, just a small Promise.all of N looping coroutines.
 *
 * @template T
 * @param {T[]} items
 * @param {number} concurrencyN
 * @param {(item: T, index: number) => Promise<void>} worker
 */
async function runPool(items, concurrencyN, worker) {
  let next = 0;
  async function loop() {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      await worker(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: concurrencyN }, () => loop()));
}

// ---------------------------------------------------------------
// Supabase + per-file ingest
// ---------------------------------------------------------------

function makeSupabaseClient() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) {
    console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env.local');
    process.exit(2);
  }
  return createClient(url, key, { auth: { persistSession: false } });
}

/**
 * Upload one PDF to Storage and upsert one catalog row.
 * Returns { ok: true } or { ok: false, error: string }.
 *
 * Failures are surfaced per-file but never thrown — the script's job
 * is to ingest as much as it can and report what didn't make it.
 *
 * @param {ReturnType<typeof makeSupabaseClient>} supabase
 * @param {{parsed: import('./spoi_parse_filename.mjs').ParsedSpoi, absPath: string, sizeBytes: number}} w
 */
async function ingestOne(supabase, w) {
  const dest = storagePath(w.parsed);
  let body;
  try {
    body = fs.readFileSync(w.absPath);
  } catch (err) {
    return { ok: false, error: `read failed: ${err instanceof Error ? err.message : String(err)}` };
  }

  // Storage upload — upsert overwrites the slot (latest filing wins).
  const upload = await supabase.storage.from(STORAGE_BUCKET).upload(dest, body, {
    contentType: 'application/pdf',
    upsert: true,
  });
  if (upload.error) {
    return { ok: false, error: `storage: ${upload.error.message}` };
  }

  // Catalog upsert — natural key keeps one row per tuple.
  const row = {
    reinsurance_year:    w.parsed.year,
    state_code:          w.parsed.state_code,
    county_code:         w.parsed.county_code,
    insurance_plan_code: w.parsed.insurance_plan_code,
    commodity_code:      w.parsed.commodity_code,
    filing_date:         w.parsed.filing_date,
    storage_path:        dest,
    file_size_bytes:     w.sizeBytes,
    source_filename:     path.basename(w.absPath),
    ingested_at:         new Date().toISOString(),
  };
  const upsert = await supabase
    .from(CATALOG_TABLE)
    .upsert(row, { onConflict: 'reinsurance_year,state_code,county_code,insurance_plan_code,commodity_code' });
  if (upsert.error) {
    return { ok: false, error: `catalog: ${upsert.error.message}` };
  }

  return { ok: true };
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
