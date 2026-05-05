// Fast bulk loader specifically for A01040 — uses CONCURRENT supabase-js
// upserts (instead of sequential) to overcome HTTP-RTT-bound throughput
// of the standard loader.mjs. Resumes from the same checkpoint file
// loader.mjs writes, so the two are interchangeable mid-load.
//
// Usage: node scripts/_fast_load_a01040.mjs [--concurrency N] [--batch N]
// Defaults: concurrency=8, batch=5000.

import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { RECORD_CONFIGS, parseRow } from '../loader.mjs';

dotenv.config({ path: '.env.local' });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env.local');
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const FILE_PATH = 'data/adm/2026/ytd/2026_A01040_CoverageLevelDifferential_YTD.txt';
const CHECKPOINT_DIR = './data/.checkpoints';
const CHECKPOINT_PATH = path.join(
  CHECKPOINT_DIR,
  path.basename(FILE_PATH).replace(/[^a-zA-Z0-9_.-]/g, '_') + '.json',
);

const args = process.argv.slice(2);
const argMap = new Map();
for (let i = 0; i < args.length; i += 2) argMap.set(args[i], args[i + 1]);
const CONCURRENCY = Number(argMap.get('--concurrency') ?? 8);
const BATCH_SIZE = Number(argMap.get('--batch') ?? 5000);

const cfg = RECORD_CONFIGS.A01040;
if (!cfg) throw new Error('A01040 not registered');

function readCheckpoint() {
  if (!fs.existsSync(CHECKPOINT_PATH)) return { lineNumber: 0, rowsUpserted: 0 };
  return JSON.parse(fs.readFileSync(CHECKPOINT_PATH, 'utf-8'));
}

function writeCheckpoint(lineNumber, rowsUpserted) {
  if (!fs.existsSync(CHECKPOINT_DIR)) fs.mkdirSync(CHECKPOINT_DIR, { recursive: true });
  fs.writeFileSync(
    CHECKPOINT_PATH,
    JSON.stringify({ lineNumber, rowsUpserted, updatedAt: new Date().toISOString() }),
  );
}

function dedupeBatch(rows) {
  const conflictCols = cfg.conflictColumns.split(',');
  const seen = new Map();
  for (const row of rows) {
    const key = conflictCols.map((c) => row[c] ?? '').join('|');
    seen.set(key, row);
  }
  return Array.from(seen.values());
}

async function upsertBatch(rows, attempt = 1) {
  const deduped = dedupeBatch(rows);
  const { error } = await supabase
    .from(cfg.table)
    .upsert(deduped, { onConflict: cfg.conflictColumns });
  if (error) {
    // Retry transient deadlocks / serialization conflicts. PG raises
    // these when concurrent upserts try to lock the same index keys in
    // different orders — they're benign at this layer (the upsert is
    // idempotent), so back off and retry.
    const transient = /deadlock|could not serialize|connection|timeout|canceling statement|fetch failed/i.test(error.message);
    if (transient && attempt <= 8) {
      const backoff = 1000 * 2 ** (attempt - 1) + Math.floor(Math.random() * 500);
      await new Promise((r) => setTimeout(r, backoff));
      return upsertBatch(rows, attempt + 1);
    }
    throw new Error(error.message);
  }
  return deduped.length;
}

async function main() {
  const start = Date.now();
  const checkpoint = readCheckpoint();
  const skipToLine = checkpoint.lineNumber;
  let rowsUpserted = checkpoint.rowsUpserted;

  console.log(`\n  fast_load_a01040`);
  console.log(`  file:        ${FILE_PATH}`);
  console.log(`  table:       ${cfg.table}`);
  console.log(`  concurrency: ${CONCURRENCY}`);
  console.log(`  batch size:  ${BATCH_SIZE}`);
  if (skipToLine > 0) {
    console.log(`  resuming from line ${skipToLine.toLocaleString()} (${rowsUpserted.toLocaleString()} already upserted)\n`);
  }

  const stream = fs.createReadStream(FILE_PATH, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let currentLine = 0;
  let rowsProcessed = skipToLine;
  let batch = [];
  const inflight = new Set();
  let lastLog = Date.now();
  const filename = path.basename(FILE_PATH);
  let highWaterEndLine = skipToLine;

  const flushOne = async () => {
    const endLine = currentLine;
    const myBatch = batch;
    batch = [];
    const job = (async () => {
      const n = await upsertBatch(myBatch);
      return { endLine, n };
    })();
    inflight.add(job);
    job.finally(() => inflight.delete(job));
    if (inflight.size >= CONCURRENCY) {
      const done = await Promise.race(inflight);
      rowsUpserted += done.n;
      if (done.endLine > highWaterEndLine) highWaterEndLine = done.endLine;
      writeCheckpoint(highWaterEndLine, rowsUpserted);
      const now = Date.now();
      if (now - lastLog > 5000) {
        const elapsed = ((now - start) / 1000).toFixed(0);
        const delta = rowsUpserted - checkpoint.rowsUpserted;
        const rate = Math.round(delta / (elapsed || 1));
        process.stdout.write(
          `\r  Processed: ${rowsProcessed.toLocaleString()} | Upserted: ${rowsUpserted.toLocaleString()} | ${rate}/s | ${elapsed}s elapsed         `,
        );
        lastLog = now;
      }
    }
  };

  for await (const line of rl) {
    currentLine++;
    if (currentLine === 1) continue;
    if (currentLine <= skipToLine) continue;
    const trimmed = line.trim();
    if (!trimmed) continue;

    const values = trimmed.split('|');
    const row = parseRow(values, cfg, filename);
    batch.push(row);
    rowsProcessed++;

    if (batch.length >= BATCH_SIZE) {
      await flushOne();
    }
  }

  if (batch.length > 0) await flushOne();

  // Drain remaining
  while (inflight.size > 0) {
    const done = await Promise.race(inflight);
    rowsUpserted += done.n;
    if (done.endLine > highWaterEndLine) highWaterEndLine = done.endLine;
    writeCheckpoint(highWaterEndLine, rowsUpserted);
  }

  // Clear checkpoint on success
  if (fs.existsSync(CHECKPOINT_PATH)) fs.unlinkSync(CHECKPOINT_PATH);

  const total = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n\n  DONE — processed ${rowsProcessed.toLocaleString()}, upserted ${rowsUpserted.toLocaleString()} in ${total}s\n`);
}

main().catch((err) => {
  console.error('\nFatal:', err.message);
  process.exit(1);
});
