import { NextRequest, NextResponse } from 'next/server';
import { RemoteFileInfo } from '@/lib/rma/types';
import { downloadFile } from '@/lib/rma/downloader';
import { parseAndLoadFile } from '@/lib/rma/parser';

export async function POST(request: NextRequest) {
  try {
    const { files, parseAfterDownload = false } = (await request.json()) as {
      files: RemoteFileInfo[];
      parseAfterDownload?: boolean;
    };

    if (!files || !Array.isArray(files) || files.length === 0) {
      return NextResponse.json(
        { error: 'No files specified' },
        { status: 400 }
      );
    }

    const results = [];

    // Download sequentially
    for (const file of files) {
      try {
        const entry = await downloadFile(file);
        let parseResult = null;

        if (parseAfterDownload) {
          try {
            parseResult = await parseAndLoadFile(entry);
          } catch (parseErr) {
            parseResult = { error: (parseErr as Error).message };
          }
        }

        results.push({
          filename: file.filename,
          status: 'downloaded',
          entry,
          parseResult,
        });
      } catch (err) {
        results.push({
          filename: file.filename,
          status: 'error',
          error: (err as Error).message,
        });
      }
    }

    const downloaded = results.filter((r) => r.status === 'downloaded').length;
    const failed = results.filter((r) => r.status === 'error').length;

    return NextResponse.json({
      summary: { total: files.length, downloaded, failed },
      results,
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 }
    );
  }
}

export const maxDuration = 300; // 5 min timeout for large downloads
