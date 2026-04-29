import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { RemoteFileInfo, ManifestEntry } from './types';
import { addManifestEntry, manifestKey } from './manifest';

const DATA_DIR = process.env.DATA_DIR || './data';

/**
 * Download a single file from RMA, save to local data/ directory.
 * Returns the manifest entry for the downloaded file.
 */
export async function downloadFile(
  remote: RemoteFileInfo
): Promise<ManifestEntry> {
  // Build local path: data/{source}/{year?}/{filename}
  const parts = [DATA_DIR, remote.source];
  if (remote.year) parts.push(String(remote.year));
  const dir = path.join(...parts);
  const localPath = path.join(dir, remote.filename);
  const relativePath = path.relative(DATA_DIR, localPath);

  // Ensure directory exists
  fs.mkdirSync(dir, { recursive: true });

  // Download with streaming
  const response = await fetch(remote.url, {
    headers: { 'User-Agent': 'CropIQ/1.0 (USDA RMA Data Tool)' },
  });

  if (!response.ok) {
    throw new Error(`Download failed: HTTP ${response.status} for ${remote.url}`);
  }

  if (!response.body) {
    throw new Error(`No response body for ${remote.url}`);
  }

  // Stream to temp file, compute checksum along the way
  const tmpPath = localPath + '.tmp';
  const hash = crypto.createHash('sha256');
  const writeStream = fs.createWriteStream(tmpPath);
  let downloadedBytes = 0;

  const reader = response.body.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      hash.update(value);
      writeStream.write(Buffer.from(value));
      downloadedBytes += value.length;
    }
  } finally {
    writeStream.end();
    // Wait for the write stream to finish
    await new Promise<void>((resolve, reject) => {
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    });
  }

  // Atomic rename
  fs.renameSync(tmpPath, localPath);

  const checksum = hash.digest('hex');

  const entry: ManifestEntry = {
    filename: remote.filename,
    source: remote.source,
    year: remote.year,
    localPath: relativePath,
    remoteUrl: remote.url,
    remoteDate: remote.remoteDate,
    remoteSize: remote.remoteSize,
    downloadedAt: new Date().toISOString(),
    fileSizeBytes: downloadedBytes,
    checksum,
    parsed: false,
    parsedAt: null,
  };

  // Update manifest
  const key = manifestKey(remote.source, remote.filename);
  addManifestEntry(key, entry);

  return entry;
}

/**
 * Download multiple files sequentially.
 */
export async function downloadBatch(
  files: RemoteFileInfo[],
  onProgress?: (completed: number, total: number, current: string) => void
): Promise<{ downloaded: ManifestEntry[]; errors: Array<{ file: string; error: string }> }> {
  const downloaded: ManifestEntry[] = [];
  const errors: Array<{ file: string; error: string }> = [];

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    onProgress?.(i, files.length, file.filename);

    try {
      const entry = await downloadFile(file);
      downloaded.push(entry);
    } catch (err) {
      errors.push({
        file: file.filename,
        error: (err as Error).message,
      });
    }
  }

  onProgress?.(files.length, files.length, 'Done');
  return { downloaded, errors };
}
