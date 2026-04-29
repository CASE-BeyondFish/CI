import { NextRequest, NextResponse } from 'next/server';
import { readManifest, updateLastScan } from '@/lib/rma/manifest';
import { scanSource, scanAllSources } from '@/lib/rma/scanner';
import { getSourceConfig, DATA_SOURCES } from '@/lib/rma/sources';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json().catch(() => ({}));
    const { sources } = body as { sources?: string[] };
    const manifest = readManifest();

    let results;

    if (sources && sources.length > 0) {
      // Scan specific sources
      results = {} as Record<string, unknown>;
      for (const sourceId of sources) {
        const config = getSourceConfig(sourceId);
        if (!config) {
          results[sourceId] = { error: `Unknown source: ${sourceId}` };
          continue;
        }
        results[sourceId] = await scanSource(config, manifest);
      }
    } else {
      // Scan all
      results = await scanAllSources(manifest);
    }

    updateLastScan();

    // Build summary
    const summary = {
      scannedAt: new Date().toISOString(),
      sources: Object.entries(results).map(([id, diff]) => {
        const d = diff as { newFiles: unknown[]; updatedFiles: unknown[]; unchangedCount: number; errors: string[] };
        return {
          id,
          label: DATA_SOURCES.find((s) => s.id === id)?.label ?? id,
          newCount: d.newFiles?.length ?? 0,
          updatedCount: d.updatedFiles?.length ?? 0,
          unchangedCount: d.unchangedCount ?? 0,
          errorCount: d.errors?.length ?? 0,
        };
      }),
    };

    return NextResponse.json({ summary, results });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 }
    );
  }
}
