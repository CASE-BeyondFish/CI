import { NextRequest, NextResponse } from 'next/server';
import { parseRawTextFile } from '@/lib/rma/parser';
import { createJob, updateJob } from '@/lib/rma/loadStatus';
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

    for (const relPath of files) {
      const normalized = path.normalize(relPath).replace(/\\/g, '/');
      if (normalized.includes('..')) continue;

      const fullPath = path.join(DATA_DIR, normalized);
      if (!fs.existsSync(fullPath)) continue;
      if (!fullPath.endsWith('.txt') && !fullPath.endsWith('.TXT')) continue;

      const jobId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const job = createJob(jobId, relPath);
      jobIds.push(jobId);

      // Fire and forget — runs in background, doesn't block the response
      (async () => {
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
          } else {
            updateJob(jobId, {
              status: 'done',
              errors: ['Unsupported record type'],
              completedAt: new Date().toISOString(),
            });
          }
        } catch (err) {
          updateJob(jobId, {
            status: 'error',
            errors: [(err as Error).message],
            completedAt: new Date().toISOString(),
          });
        }
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
