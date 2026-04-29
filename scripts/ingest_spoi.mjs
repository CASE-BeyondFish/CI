/**
 * One-shot ingest of Special Provisions PDFs.
 *
 * Walks data/special_provisions/ recursively, dedups to "latest filing
 * per (year, state, county, plan, commodity)", uploads each survivor's
 * PDF to the spoi-documents Supabase Storage bucket, and upserts a row
 * in public.spoi_documents.
 *
 * Usage:
 *   node scripts/ingest_spoi.mjs                  # all years, real run
 *   node scripts/ingest_spoi.mjs --dry-run        # walk + dedup + summary, no writes
 *   node scripts/ingest_spoi.mjs --year=2026      # restrict to one year
 *   node scripts/ingest_spoi.mjs --year=2026 --dry-run
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
import { parseSpoiPath } from './spoi_parse_filename.mjs';

dotenv.config({ path: '.env.local' });

const DATA_ROOT = path.resolve('data/special_provisions');

// ---------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const yearArg = args.find((a) => a.startsWith('--year='));
const yearFilter = yearArg ? Number(yearArg.split('=')[1]) : null;

if (yearArg && (!Number.isInteger(yearFilter) || yearFilter < 1900 || yearFilter > 2100)) {
  console.error(`bad --year value: ${yearArg}`);
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

  console.log(`\nLive run is wired in a later commit. Re-run with --dry-run for now.`);
  process.exit(1);
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
