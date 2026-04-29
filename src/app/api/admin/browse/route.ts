import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR || './data';

interface FileEntry {
  name: string;
  path: string;        // relative to DATA_DIR
  type: 'file' | 'directory';
  size: number;
  recordType: string | null;  // e.g. "A00810" extracted from filename
  recordName: string | null;  // e.g. "Price" extracted from filename
}

function detectRecordInfo(filename: string): { code: string | null; name: string | null } {
  const match = filename.match(/A(\d{5})_([^_]+)/);
  if (!match) return { code: null, name: null };
  return { code: `A${match[1]}`, name: match[2] };
}

function listDirectory(dirPath: string, relativeTo: string): FileEntry[] {
  if (!fs.existsSync(dirPath)) return [];

  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  const results: FileEntry[] = [];

  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name);
    const relPath = path.relative(relativeTo, fullPath).replace(/\\/g, '/');

    if (entry.isDirectory()) {
      results.push({
        name: entry.name,
        path: relPath,
        type: 'directory',
        size: 0,
        recordType: null,
        recordName: null,
      });
    } else if (entry.name.endsWith('.txt') || entry.name.endsWith('.TXT') || entry.name.endsWith('.zip') || entry.name.endsWith('.ZIP')) {
      const stats = fs.statSync(fullPath);
      const { code, name } = detectRecordInfo(entry.name);
      results.push({
        name: entry.name,
        path: relPath,
        type: 'file',
        size: stats.size,
        recordType: code,
        recordName: name,
      });
    }
  }

  // Sort: directories first, then files
  results.sort((a, b) => {
    if (a.type !== b.type) return a.type === 'directory' ? -1 : 1;
    return a.name.localeCompare(b.name);
  });

  return results;
}

export async function GET(request: NextRequest) {
  try {
    const dir = request.nextUrl.searchParams.get('dir') || '';

    // Prevent directory traversal
    const normalized = path.normalize(dir).replace(/\\/g, '/');
    if (normalized.includes('..')) {
      return NextResponse.json({ error: 'Invalid path' }, { status: 400 });
    }

    const fullPath = path.join(DATA_DIR, normalized);
    const entries = listDirectory(fullPath, DATA_DIR);

    return NextResponse.json({
      currentDir: normalized || '/',
      parentDir: normalized ? path.dirname(normalized).replace(/\\/g, '/') : null,
      entries,
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 }
    );
  }
}
