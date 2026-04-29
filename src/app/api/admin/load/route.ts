import { NextRequest, NextResponse } from 'next/server';
import { parseRawTextFile } from '@/lib/rma/parser';
import { createJob, updateJob, setViewRefreshState } from '@/lib/rma/loadStatus';
import { supabase } from '@/lib/db/supabase';
import { refreshCarrackYieldsViews } from '@/lib/db/refreshViews';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || './data';

export async function POST(request: NextRequest) {
  try {
    const { files } = (await request.json()) as { files: string[] };

    if (!files || !Array.isArray(files) || files.length === 0) {
      return NextResponse.json({ error: 'No files specified' }, { status: 400 });
    }

    const jobIds: string[] = [];
    // Per-file promises that resolve to how many rows the file wrote into
    // insurance_offers. We collect them so a supervisor task can wait for the
    // whole batch before deciding whether to refresh the CarrackYields MVs
    // (refreshing once mid-batch and once at end would double-hammer the DB).
    const filePromises: Promise<number>[] = [];

    for (const relPath of files) {
      const normalized = path.normalize(relPath).replace(/\\/g, '/');
      if (normalized.includes('..')) continue;

      const fullPath = path.join(DATA_DIR, normalized);
      if (!fs.existsSync(fullPath)) continue;
      if (!fullPath.endsWith('.txt') && !fullPath.endsWith('.TXT')) continue;

      const jobId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      createJob(jobId, relPath);
      jobIds.push(jobId);

      // Fire and forget — runs in background, doesn't block the response.
      // Returns the rows-written-to-insurance_offers count for the supervisor.
      const filePromise: Promise<number> = (async () => {
        try {
          const result = await parseRawTextFile(fullPath, path.basename(relPath), (processed, upserted) => {
            updateJob(jobId, { rowsProcessed: processed, rowsUpserted: upserted });
          });

          if (result) {
            updateJob(jobId, {
              status: result.errors.length > 0 ? 'error' : 'done',
              recordType: result.recordType,
              table: result.table,
              rowsProcessed: result.rowsProcessed,
              rowsUpserted: result.rowsUpserted,
              errors: result.errors,
              completedAt: new Date().toISOString(),
            });
            return result.table === 'insurance_offers' ? result.rowsUpserted : 0;
          } else {
            updateJob(jobId, {
              status: 'done',
              errors: ['Unsupported record type'],
              completedAt: new Date().toISOString(),
            });
            return 0;
          }
        } catch (err) {
          updateJob(jobId, {
            status: 'error',
            errors: [(err as Error).message],
            completedAt: new Date().toISOString(),
          });
          return 0;
        }
      })();

      filePromises.push(filePromise);
    }

    // Supervisor: wait for every file in this batch to settle, then refresh
    // the CarrackYields MVs once if any file wrote rows to insurance_offers.
    // This runs in the background after the response has gone out.
    if (filePromises.length > 0) {
      void (async () => {
        const settled = await Promise.allSettled(filePromises);
        const totalOffersUpserted = settled.reduce(
          (acc, s) => acc + (s.status === 'fulfilled' ? s.value : 0),
          0
        );

        if (totalOffersUpserted <= 0) return;

        setViewRefreshState({ status: 'running', startedAt: new Date().toISOString() });
        const result = await refreshCarrackYieldsViews(supabase);
        setViewRefreshState({
          status: result.success ? 'done' : 'error',
          completedAt: new Date().toISOString(),
          result,
        });
      })();
    }

    // Return immediately with job IDs
    return NextResponse.json({
      message: `Started ${jobIds.length} load job(s)`,
      jobIds,
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 }
    );
  }
}
