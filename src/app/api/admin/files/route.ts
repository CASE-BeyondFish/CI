import { NextRequest, NextResponse } from 'next/server';
import { readManifest } from '@/lib/rma/manifest';

export async function GET(request: NextRequest) {
  try {
    const manifest = readManifest();
    const source = request.nextUrl.searchParams.get('source');

    let files = Object.values(manifest.files);

    if (source) {
      files = files.filter((f) => f.source === source);
    }

    // Sort by downloadedAt descending
    files.sort((a, b) => new Date(b.downloadedAt).getTime() - new Date(a.downloadedAt).getTime());

    return NextResponse.json({
      lastFullScan: manifest.lastFullScan,
      totalFiles: files.length,
      files,
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 }
    );
  }
}
