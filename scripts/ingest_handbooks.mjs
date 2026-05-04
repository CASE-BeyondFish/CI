/**
 * One-shot ingest of RMA handbook PDFs.
 *
 * Reads scripts/handbooks-manifest.json, verifies each PDF exists at
 * {handbooks_root}/{filename}, validates the manifest as a whole, then
 * uploads each PDF to the `handbooks` Supabase Storage bucket and
 * upserts a row in public.handbooks.
 *
 * Usage:
 *   node scripts/ingest_handbooks.mjs                          # all entries, real run
 *   node scripts/ingest_handbooks.mjs --dry-run                # validate only, no writes
 *   node scripts/ingest_handbooks.mjs --filter=CIH             # only CIH handbook(s)
 *   node scripts/ingest_handbooks.mjs --root=D:\\handbooks     # override local root
 *
 * Env (loaded from .env.local at the project root):
 *   SUPABASE_URL          required for non-dry-run
 *   SUPABASE_SERVICE_KEY  required for non-dry-run (writes need service role)
 *   HANDBOOKS_ROOT        local handbook directory (default: R:\\CarrackReferences\\)
 *
 * Re-running is safe: storage uploads use upsert, DB writes upsert on
 * the natural key (document_short, document_version).
 */

import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

dotenv.config({ path: '.env.local' });

const MANIFEST_PATH = path.resolve('scripts/handbooks-manifest.json');
const STORAGE_BUCKET = 'handbooks';
const CATALOG_TABLE = 'handbooks';
const MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024; // 50MB, matches bucket limit

// ---------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const filterArg = args.find((a) => a.startsWith('--filter='));
const filterShort = filterArg ? filterArg.split('=')[1] : null;
const rootArg = args.find((a) => a.startsWith('--root='));
const handbooksRoot = path.resolve(
  rootArg ? rootArg.split('=')[1] : (process.env.HANDBOOKS_ROOT ?? 'R:\\CarrackReferences\\')
);

// ---------------------------------------------------------------
// Manifest load + validate
// ---------------------------------------------------------------

/**
 * @typedef {Object} ManifestEntry
 * @property {string} filename
 * @property {string} document_short
 * @property {string} document_full
 * @property {string | null} fcic_number
 * @property {string} document_version
 * @property {number | null} reinsurance_year
 * @property {string | null} rma_source_url
 * @property {string | null} notes
 */

/** @returns {ManifestEntry[]} */
function loadManifest() {
  if (!fs.existsSync(MANIFEST_PATH)) {
    console.error(`manifest not found: ${MANIFEST_PATH}`);
    process.exit(2);
  }
  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(MANIFEST_PATH, 'utf8'));
  } catch (err) {
    console.error(`manifest is not valid JSON: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(2);
  }
  if (!raw || !Array.isArray(raw.handbooks)) {
    console.error(`manifest must contain { "handbooks": [...] }`);
    process.exit(2);
  }
  return raw.handbooks;
}

/**
 * Storage path convention:
 *   {document_short_lower}/{document_version}/{filename}
 *
 * @param {ManifestEntry} entry
 */
function storagePath(entry) {
  return `${entry.document_short.toLowerCase()}/${entry.document_version}/${entry.filename}`;
}

/**
 * Walk the manifest, validate every entry, and return the prepared
 * ingest items plus an error list. The script aborts before any write
 * if errors.length > 0.
 *
 * @param {ManifestEntry[]} entries
 * @returns {{ items: { entry: ManifestEntry, absPath: string, sizeBytes: number }[], errors: string[] }}
 */
function validate(entries) {
  /** @type {string[]} */
  const errors = [];
  /** @type {{ entry: ManifestEntry, absPath: string, sizeBytes: number }[]} */
  const items = [];
  /** @type {Map<string, number>} */
  const seenKeys = new Map();

  entries.forEach((entry, idx) => {
    const where = `manifest[${idx}]`;
    let entryHasError = false;

    for (const field of ['filename', 'document_short', 'document_full', 'document_version']) {
      const val = entry?.[field];
      if (typeof val !== 'string' || val.trim().length === 0) {
        errors.push(`${where}: missing or empty required field "${field}"`);
        entryHasError = true;
      }
    }
    // Bail on this entry if required strings are missing — downstream
    // checks (file existence, dup keys) would just produce noise.
    if (entryHasError) return;

    // Natural-key duplicate check
    const key = `${entry.document_short}|${entry.document_version}`;
    const prevIdx = seenKeys.get(key);
    if (prevIdx !== undefined) {
      errors.push(
        `${where}: duplicate (document_short, document_version) = (${entry.document_short}, ${entry.document_version}); ` +
        `first seen at manifest[${prevIdx}]`
      );
      return;
    }
    seenKeys.set(key, idx);

    // File-on-disk check
    const absPath = path.join(handbooksRoot, entry.filename);
    let sizeBytes;
    try {
      const stat = fs.statSync(absPath);
      if (!stat.isFile()) {
        errors.push(`${where}: path exists but is not a file: ${absPath}`);
        return;
      }
      sizeBytes = stat.size;
    } catch {
      errors.push(`${where}: file not found at ${absPath}`);
      return;
    }

    if (sizeBytes > MAX_FILE_SIZE_BYTES) {
      const mb = (sizeBytes / (1024 * 1024)).toFixed(1);
      errors.push(`${where}: file ${entry.filename} is ${mb}MB, exceeds 50MB bucket limit`);
      return;
    }
    if (sizeBytes === 0) {
      errors.push(`${where}: file ${entry.filename} is 0 bytes`);
      return;
    }

    items.push({ entry, absPath, sizeBytes });
  });

  return { items, errors };
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------

async function main() {
  const startedAt = Date.now();
  console.log(`Handbooks ingest — ${dryRun ? 'DRY RUN' : 'LIVE'}${filterShort ? ` (filter=${filterShort})` : ''}`);
  console.log(`  manifest:       ${MANIFEST_PATH}`);
  console.log(`  handbooks root: ${handbooksRoot}`);

  const allEntries = loadManifest();
  const filtered = filterShort
    ? allEntries.filter((e) => e?.document_short === filterShort)
    : allEntries;

  if (filterShort && filtered.length === 0) {
    console.error(`\nfilter=${filterShort} matched 0 entries in the manifest`);
    process.exit(2);
  }

  console.log(`\nValidating ${filtered.length} manifest entr${filtered.length === 1 ? 'y' : 'ies'}...`);
  const { items, errors } = validate(filtered);

  if (errors.length > 0) {
    console.error(`\nManifest validation failed with ${errors.length} error${errors.length === 1 ? '' : 's'}:`);
    for (const e of errors) console.error(`  - ${e}`);
    console.error(`\nNo uploads attempted. Fix the manifest (or filenames on disk) and re-run.`);
    process.exit(1);
  }

  console.log(`  ${items.length} entr${items.length === 1 ? 'y' : 'ies'} validated`);
  for (const { entry, sizeBytes } of items) {
    const kb = (sizeBytes / 1024).toFixed(0);
    console.log(`    ${entry.document_short.padEnd(15)} ${entry.document_version.padEnd(10)} ${kb.padStart(6)} KB  ${entry.filename}`);
  }

  if (dryRun) {
    console.log(`\nDry run — no uploads, no DB writes. Done in ${((Date.now() - startedAt) / 1000).toFixed(1)}s.`);
    return;
  }

  // ---------------------------------------------------------------
  // Live run — sequential ingest. Handbook ingests are rare and small
  // (a handful of files); no need for concurrency.
  // ---------------------------------------------------------------

  const supabase = makeSupabaseClient();
  console.log(
    `\nLive run — uploading to bucket "${STORAGE_BUCKET}", upserting into ${CATALOG_TABLE}...`
  );

  let uploaded = 0;
  let failed = 0;
  /** @type {{ filename: string, error: string }[]} */
  const failures = [];

  for (const item of items) {
    process.stdout.write(`  ${item.entry.document_short} ${item.entry.document_version} ... `);
    const result = await ingestOne(supabase, item);
    if (result.ok) {
      uploaded++;
      console.log('ok');
    } else {
      failed++;
      failures.push({ filename: item.entry.filename, error: result.error });
      console.log(`FAIL — ${result.error}`);
    }
  }

  console.log(`\nDone in ${((Date.now() - startedAt) / 1000).toFixed(1)}s. ingested=${uploaded} failed=${failed}`);
  if (failures.length > 0) {
    console.log(`\nFailures:`);
    for (const f of failures) {
      console.log(`  ${f.filename}\n    ${f.error}`);
    }
    process.exit(1);
  }
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
 * @param {ReturnType<typeof makeSupabaseClient>} supabase
 * @param {{ entry: ManifestEntry, absPath: string, sizeBytes: number }} item
 */
async function ingestOne(supabase, item) {
  const { entry, absPath, sizeBytes } = item;
  const dest = storagePath(entry);

  let body;
  try {
    body = fs.readFileSync(absPath);
  } catch (err) {
    return { ok: false, error: `read failed: ${err instanceof Error ? err.message : String(err)}` };
  }

  // Storage upload — upsert overwrites the slot (latest curated PDF wins).
  const upload = await supabase.storage.from(STORAGE_BUCKET).upload(dest, body, {
    contentType: 'application/pdf',
    upsert: true,
  });
  if (upload.error) {
    return { ok: false, error: `storage: ${upload.error.message}` };
  }

  // Catalog upsert — natural key keeps one row per (document_short, version).
  const row = {
    document_short:   entry.document_short,
    document_full:    entry.document_full,
    fcic_number:      entry.fcic_number,
    document_version: entry.document_version,
    reinsurance_year: entry.reinsurance_year,
    storage_path:     dest,
    source_filename:  entry.filename,
    file_size_bytes:  sizeBytes,
    rma_source_url:   entry.rma_source_url,
    notes:            entry.notes,
    uploaded_at:      new Date().toISOString(),
  };
  const upsert = await supabase
    .from(CATALOG_TABLE)
    .upsert(row, { onConflict: 'document_short,document_version' });
  if (upsert.error) {
    return { ok: false, error: `catalog: ${upsert.error.message}` };
  }

  return { ok: true };
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
