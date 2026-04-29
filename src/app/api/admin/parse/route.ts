import { NextRequest, NextResponse } from 'next/server';
import { readManifest } from '@/lib/rma/manifest';
import { parseAndLoadFile } from '@/lib/rma/parser';
import { supabase } from '@/lib/db/supabase';
import { refreshCarrackYieldsViews, type RefreshResult } from '@/lib/db/refreshViews';

export async function POST(request: NextRequest) {
  try {
    const { keys } = (await request.json()) as { keys: string[] };

    if (!keys || !Array.isArray(keys) || keys.length === 0) {
      return NextResponse.json(
        { error: 'No file keys specified' },
        { status: 400 }
      );
    }

    const manifest = readManifest();
    const results = [];
    // Track rows written to insurance_offers across all files in this request.
    // Used to decide whether the CarrackYields MVs need refreshing afterward.
    let insuranceOffersUpserted = 0;

    for (const key of keys) {
      const entry = manifest.files[key];
      if (!entry) {
        results.push({ key, status: 'error', error: 'File not found in manifest' });
        continue;
      }

      if (entry.parsed) {
        results.push({ key, status: 'skipped', message: 'Already pushed to Supabase' });
        continue;
      }

      try {
        const parseResult = await parseAndLoadFile(entry);

        for (const rt of parseResult.recordTypes) {
          if (rt.table === 'insurance_offers') {
            insuranceOffersUpserted += rt.rowsUpserted;
          }
        }

        results.push({
          key,
          status: parseResult.errors.length > 0 ? 'partial' : 'success',
          recordTypes: parseResult.recordTypes.map(r => ({
            type: r.recordType,
            table: r.table,
            processed: r.rowsProcessed,
            upserted: r.rowsUpserted,
          })),
          totalProcessed: parseResult.totalProcessed,
          totalUpserted: parseResult.totalUpserted,
          errors: parseResult.errors,
        });
      } catch (err) {
        results.push({ key, status: 'error', error: (err as Error).message });
      }
    }

    const success = results.filter((r) => r.status === 'success').length;
    const partial = results.filter((r) => r.status === 'partial').length;
    const skipped = results.filter((r) => r.status === 'skipped').length;
    const failed = results.filter((r) => r.status === 'error').length;

    // Refresh CarrackYields MVs only if at least one row landed in insurance_offers.
    // The helper catches its own errors — refresh failure must not fail the parse.
    let viewRefresh: RefreshResult | null = null;
    if (insuranceOffersUpserted > 0) {
      viewRefresh = await refreshCarrackYieldsViews(supabase);
    }

    return NextResponse.json({
      summary: { total: keys.length, success, partial, skipped, failed },
      results,
      viewRefresh,
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 }
    );
  }
}

export const maxDuration = 300;
