export type DataSource = 'adm' | 'special_provisions' | 'sob' | 'cause_of_loss';

export interface DataSourceConfig {
  id: DataSource;
  label: string;
  description: string;
  baseUrl: string;
  listingStrategy: 'year-based' | 'flat';
  yearRange?: { start: number; end: number };
  localDir: string;
  filePattern: RegExp;
}

export interface RemoteFileInfo {
  url: string;
  filename: string;
  source: DataSource;
  year: number | null;
  remoteDate: string;       // ISO string parsed from directory listing
  remoteSize: number;       // bytes
}

export interface ManifestEntry {
  filename: string;
  source: DataSource;
  year: number | null;
  localPath: string;        // relative to data/ dir
  remoteUrl: string;
  remoteDate: string;
  remoteSize: number;
  downloadedAt: string;     // ISO timestamp
  fileSizeBytes: number;
  checksum: string;         // SHA-256
  parsed: boolean;          // whether data was loaded into DB
  parsedAt: string | null;
}

export interface Manifest {
  version: 1;
  lastFullScan: string | null;
  files: Record<string, ManifestEntry>; // keyed by "{source}/{filename}"
}

export interface ScanDiff {
  source: DataSource;
  newFiles: RemoteFileInfo[];
  updatedFiles: (RemoteFileInfo & { existing: ManifestEntry })[];
  unchangedCount: number;
  errors: string[];
}
