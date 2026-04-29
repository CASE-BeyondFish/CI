import { DataSourceConfig, RemoteFileInfo, ScanDiff, Manifest } from './types';
import { DATA_SOURCES } from './sources';
import { manifestKey } from './manifest';

/**
 * Parse an HTML directory listing from the RMA server.
 * The server renders Apache-style HTML with <a> tags for files,
 * followed by date and size text.
 */
export function parseDirectoryListing(
  html: string,
  baseUrl: string,
  source: DataSourceConfig,
  year: number | null
): RemoteFileInfo[] {
  const files: RemoteFileInfo[] = [];

  // RMA format: MM/DD/YYYY HH:MM AM     SIZE <a href="./filename">filename</a>
  const lineRegex = /(\d{1,2}\/\d{1,2}\/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)\s+([\d]+)\s+<a\s+href="([^"]+)">([^<]+)<\/a>/gi;

  let match;
  while ((match = lineRegex.exec(html)) !== null) {
    const dateStr = match[1].trim();
    const sizeStr = match[2].replace(/,/g, '');
    const href = match[3];
    const filename = match[4].trim();

    // Skip parent directory links
    if (filename === '..') continue;
    // Skip directories (they end with /)
    if (href.endsWith('/')) continue;
    // Apply file pattern filter
    if (!source.filePattern.test(filename)) continue;

    const remoteDate = parseCSTDate(dateStr);
    const remoteSize = parseInt(sizeStr, 10);

    const base = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
    const url = `${base}${encodeURIComponent(filename)}`;

    files.push({
      url,
      filename,
      source: source.id,
      year,
      remoteDate,
      remoteSize,
    });
  }

  return files;
}

/**
 * Parse RMA CST date string like "03/05/2026 12:34 PM" into ISO string.
 */
function parseCSTDate(dateStr: string): string {
  // RMA shows dates in CST (UTC-6)
  const [datePart, timePart, ampm] = dateStr.split(/\s+/);
  const [month, day, year] = datePart.split('/').map(Number);
  let [hours, minutes] = timePart.split(':').map(Number);

  if (ampm.toUpperCase() === 'PM' && hours !== 12) hours += 12;
  if (ampm.toUpperCase() === 'AM' && hours === 12) hours = 0;

  // Construct as CST (UTC-6)
  const isoStr = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}T${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:00-06:00`;
  return new Date(isoStr).toISOString();
}

/**
 * Fetch and parse a single directory listing from RMA.
 */
async function fetchDirectoryListing(
  url: string,
  source: DataSourceConfig,
  year: number | null
): Promise<{ files: RemoteFileInfo[]; error: string | null }> {
  try {
    const response = await fetch(url, {
      headers: { 'User-Agent': 'CropIQ/1.0 (USDA RMA Data Tool)' },
    });
    if (!response.ok) {
      return { files: [], error: `HTTP ${response.status} for ${url}` };
    }
    const html = await response.text();
    const files = parseDirectoryListing(html, url, source, year);
    return { files, error: null };
  } catch (err) {
    return { files: [], error: `Failed to fetch ${url}: ${(err as Error).message}` };
  }
}

/**
 * Scan a single data source against the manifest.
 */
export async function scanSource(
  source: DataSourceConfig,
  manifest: Manifest
): Promise<ScanDiff> {
  const diff: ScanDiff = {
    source: source.id,
    newFiles: [],
    updatedFiles: [],
    unchangedCount: 0,
    errors: [],
  };

  let allRemoteFiles: RemoteFileInfo[] = [];

  if (source.listingStrategy === 'year-based' && source.yearRange) {
    // Scan each year directory
    for (let year = source.yearRange.start; year <= source.yearRange.end; year++) {
      const url = `${source.baseUrl}/${year}/`;
      const { files, error } = await fetchDirectoryListing(url, source, year);
      if (error) {
        diff.errors.push(error);
      }
      allRemoteFiles = allRemoteFiles.concat(files);
    }
  } else {
    // Flat listing
    const url = source.baseUrl.endsWith('/') ? source.baseUrl : `${source.baseUrl}/`;
    const { files, error } = await fetchDirectoryListing(url, source, null);
    if (error) {
      diff.errors.push(error);
    }
    allRemoteFiles = files;
  }

  // Compare against manifest
  for (const remote of allRemoteFiles) {
    const key = manifestKey(source.id, remote.filename);
    const existing = manifest.files[key];

    if (!existing) {
      diff.newFiles.push(remote);
    } else if (
      existing.remoteDate !== remote.remoteDate ||
      existing.remoteSize !== remote.remoteSize
    ) {
      diff.updatedFiles.push({ ...remote, existing });
    } else {
      diff.unchangedCount++;
    }
  }

  return diff;
}

/**
 * Scan all data sources.
 */
export async function scanAllSources(
  manifest: Manifest
): Promise<Record<string, ScanDiff>> {
  const results: Record<string, ScanDiff> = {};

  // Scan sources sequentially to be polite to the USDA server
  for (const source of DATA_SOURCES) {
    results[source.id] = await scanSource(source, manifest);
  }

  return results;
}
