import fs from 'fs';
import path from 'path';
import { Manifest, ManifestEntry } from './types';

const DATA_DIR = process.env.DATA_DIR || './data';

function manifestPath(): string {
  return path.join(DATA_DIR, 'manifest.json');
}

export function readManifest(): Manifest {
  const filePath = manifestPath();
  if (!fs.existsSync(filePath)) {
    return { version: 1, lastFullScan: null, files: {} };
  }
  const raw = fs.readFileSync(filePath, 'utf-8');
  return JSON.parse(raw) as Manifest;
}

export function writeManifest(manifest: Manifest): void {
  const filePath = manifestPath();
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  // Atomic write: write to temp, then rename
  const tmpPath = filePath + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(manifest, null, 2), 'utf-8');
  fs.renameSync(tmpPath, filePath);
}

export function addManifestEntry(key: string, entry: ManifestEntry): Manifest {
  const manifest = readManifest();
  manifest.files[key] = entry;
  writeManifest(manifest);
  return manifest;
}

export function updateLastScan(): Manifest {
  const manifest = readManifest();
  manifest.lastFullScan = new Date().toISOString();
  writeManifest(manifest);
  return manifest;
}

export function manifestKey(source: string, filename: string): string {
  return `${source}/${filename}`;
}
