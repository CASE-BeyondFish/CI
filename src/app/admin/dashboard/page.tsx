'use client';

import { useState, useEffect, useCallback } from 'react';
import { useRouter } from 'next/navigation';

type DataSource = 'adm' | 'special_provisions' | 'sob' | 'cause_of_loss';

interface ManifestFile {
  filename: string;
  source: DataSource;
  year: number | null;
  localPath: string;
  remoteUrl: string;
  remoteDate: string;
  remoteSize: number;
  downloadedAt: string;
  fileSizeBytes: number;
  checksum: string;
  parsed: boolean;
  parsedAt: string | null;
}

interface RemoteFile {
  url: string;
  filename: string;
  source: DataSource;
  year: number | null;
  remoteDate: string;
  remoteSize: number;
}

interface ScanDiff {
  source: DataSource;
  newFiles: RemoteFile[];
  updatedFiles: (RemoteFile & { existing: ManifestFile })[];
  unchangedCount: number;
  errors: string[];
}

interface ScanSummary {
  scannedAt: string;
  sources: Array<{
    id: string;
    label: string;
    newCount: number;
    updatedCount: number;
    unchangedCount: number;
    errorCount: number;
  }>;
}

const SOURCE_TABS: { id: DataSource; label: string }[] = [
  { id: 'adm', label: 'ADM' },
  { id: 'special_provisions', label: 'Special Provisions' },
  { id: 'sob', label: 'Summary of Business' },
  { id: 'cause_of_loss', label: 'Cause of Loss' },
];

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export default function AdminDashboard() {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState<DataSource>('adm');
  const [activeLocalTab, setActiveLocalTab] = useState<DataSource>('adm');
  const [files, setFiles] = useState<ManifestFile[]>([]);
  const [lastScan, setLastScan] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [scanResults, setScanResults] = useState<Record<string, ScanDiff> | null>(null);
  const [scanSummary, setScanSummary] = useState<ScanSummary | null>(null);
  const [downloading, setDownloading] = useState<Set<string>>(new Set());
  const [downloadingAll, setDownloadingAll] = useState(false);
  const [selectedFiles, setSelectedFiles] = useState<Set<string>>(new Set());
  const [pushing, setPushing] = useState(false);
  const [pushResults, setPushResults] = useState<Record<string, { status: string; rowsProcessed?: number; rowsUpserted?: number; error?: string }> | null>(null);

  // Folder browser state
  const [browseDir, setBrowseDir] = useState<string>('');
  const [browseEntries, setBrowseEntries] = useState<Array<{
    name: string; path: string; type: 'file' | 'directory';
    size: number; recordType: string | null; recordName: string | null;
  }>>([]);
  const [browseParent, setBrowseParent] = useState<string | null>(null);
  const [browseLoading, setBrowseLoading] = useState(false);
  const [selectedDiskFiles, setSelectedDiskFiles] = useState<Set<string>>(new Set());
  const [loadingDisk, setLoadingDisk] = useState(false);
  const [diskJobs, setDiskJobs] = useState<Array<{
    id: string; file: string; status: string; recordType: string | null; table: string | null;
    rowsProcessed: number; rowsUpserted: number; errors: string[];
    startedAt: string; completedAt: string | null;
  }>>([]);

  const loadFiles = useCallback(async () => {
    try {
      const res = await fetch('/api/admin/files');
      if (res.status === 401) {
        router.push('/admin/login');
        return;
      }
      const data = await res.json();
      setFiles(data.files);
      setLastScan(data.lastFullScan);
    } catch (err) {
      console.error('Failed to load files:', err);
    }
  }, [router]);

  useEffect(() => {
    loadFiles();
  }, [loadFiles]);

  async function handleLogout() {
    await fetch('/api/admin/auth/logout', { method: 'POST' });
    router.push('/admin/login');
  }

  async function handleScan() {
    setScanning(true);
    setScanResults(null);
    setScanSummary(null);

    try {
      const res = await fetch('/api/admin/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
      const data = await res.json();
      setScanResults(data.results);
      setScanSummary(data.summary);
    } catch (err) {
      console.error('Scan failed:', err);
    } finally {
      setScanning(false);
    }
  }

  async function handleDownload(file: RemoteFile) {
    setDownloading((prev) => new Set(prev).add(file.filename));

    try {
      await fetch('/api/admin/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ files: [file] }),
      });
      await loadFiles();
    } catch (err) {
      console.error('Download failed:', err);
    } finally {
      setDownloading((prev) => {
        const next = new Set(prev);
        next.delete(file.filename);
        return next;
      });
    }
  }

  async function handleDownloadAll(filesToDownload: RemoteFile[]) {
    setDownloadingAll(true);

    try {
      await fetch('/api/admin/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ files: filesToDownload }),
      });
      await loadFiles();
    } catch (err) {
      console.error('Batch download failed:', err);
    } finally {
      setDownloadingAll(false);
    }
  }

  function toggleFileSelection(key: string) {
    setSelectedFiles((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }

  function toggleAllLocalFiles() {
    const localSourceFiles = files.filter((f) => f.source === activeLocalTab);
    const allKeys = localSourceFiles.map((f) => `${f.source}/${f.filename}`);
    const allSelected = allKeys.every((k) => selectedFiles.has(k));

    if (allSelected) {
      setSelectedFiles((prev) => {
        const next = new Set(prev);
        allKeys.forEach((k) => next.delete(k));
        return next;
      });
    } else {
      setSelectedFiles((prev) => {
        const next = new Set(prev);
        allKeys.forEach((k) => next.add(k));
        return next;
      });
    }
  }

  async function handlePushToSupabase() {
    if (selectedFiles.size === 0) return;
    setPushing(true);
    setPushResults(null);

    try {
      const res = await fetch('/api/admin/parse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keys: Array.from(selectedFiles) }),
      });
      const data = await res.json();

      // Build results map
      const resultsMap: Record<string, { status: string; rowsProcessed?: number; rowsUpserted?: number; error?: string }> = {};
      for (const r of data.results) {
        resultsMap[r.key] = r;
      }
      setPushResults(resultsMap);
      setSelectedFiles(new Set());
      await loadFiles();
    } catch (err) {
      console.error('Push failed:', err);
    } finally {
      setPushing(false);
    }
  }

  // Folder browser functions
  const browseTo = useCallback(async (dir: string) => {
    setBrowseLoading(true);
    try {
      const res = await fetch(`/api/admin/browse?dir=${encodeURIComponent(dir)}`);
      const data = await res.json();
      setBrowseDir(data.currentDir);
      setBrowseEntries(data.entries);
      setBrowseParent(data.parentDir);
      setSelectedDiskFiles(new Set());
    } catch (err) {
      console.error('Browse failed:', err);
    } finally {
      setBrowseLoading(false);
    }
  }, []);

  useEffect(() => {
    browseTo('');
  }, [browseTo]);

  function toggleDiskFile(filePath: string) {
    setSelectedDiskFiles(prev => {
      const next = new Set(prev);
      if (next.has(filePath)) next.delete(filePath);
      else next.add(filePath);
      return next;
    });
  }

  function toggleAllDiskFiles() {
    const txtFiles = browseEntries.filter(e => e.type === 'file' && e.name.endsWith('.txt'));
    const allPaths = txtFiles.map(e => e.path);
    const allSelected = allPaths.every(p => selectedDiskFiles.has(p));
    if (allSelected) {
      setSelectedDiskFiles(prev => {
        const next = new Set(prev);
        allPaths.forEach(p => next.delete(p));
        return next;
      });
    } else {
      setSelectedDiskFiles(prev => {
        const next = new Set(prev);
        allPaths.forEach(p => next.add(p));
        return next;
      });
    }
  }

  // Poll for job status
  useEffect(() => {
    const hasRunning = diskJobs.some(j => j.status === 'running');
    if (!hasRunning && !loadingDisk) return;

    const interval = setInterval(async () => {
      try {
        const res = await fetch('/api/admin/load/status');
        const data = await res.json();
        setDiskJobs(data.jobs);

        // Stop polling if no jobs are running
        const stillRunning = data.jobs.some((j: { status: string }) => j.status === 'running');
        if (!stillRunning) {
          setLoadingDisk(false);
        }
      } catch {
        // ignore
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [diskJobs, loadingDisk]);

  async function handleLoadFromDisk() {
    if (selectedDiskFiles.size === 0) return;
    setLoadingDisk(true);

    try {
      const res = await fetch('/api/admin/load', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ files: Array.from(selectedDiskFiles) }),
      });
      await res.json();
      setSelectedDiskFiles(new Set());

      // Immediately fetch status
      const statusRes = await fetch('/api/admin/load/status');
      const statusData = await statusRes.json();
      setDiskJobs(statusData.jobs);
    } catch (err) {
      console.error('Load failed:', err);
      setLoadingDisk(false);
    }
  }

  // Supported record types for highlighting
  const SUPPORTED_TYPES = new Set(['A00520','A00440','A00420','A00460','A00510','A00540','A00410','A00430','A00470','A00480','A00490','A00450','A00500','A00530','A00070','A00030','A00810','A00200','A01010','A01100','A01115','A01120','A01125']);

  // Check if a remote file is already downloaded
  const downloadedFilenames = new Set(files.map((f) => f.filename));

  const activeScan = scanResults?.[activeTab];
  const newAndUpdated = [
    ...(activeScan?.newFiles ?? []),
    ...(activeScan?.updatedFiles ?? []),
  ];

  const localSourceFiles = files
    .filter((f) => f.source === activeLocalTab)
    .sort((a, b) => new Date(b.downloadedAt).getTime() - new Date(a.downloadedAt).getTime());

  const localAllKeys = localSourceFiles.map((f) => `${f.source}/${f.filename}`);
  const localAllSelected = localAllKeys.length > 0 && localAllKeys.every((k) => selectedFiles.has(k));
  const selectedUnpushedCount = Array.from(selectedFiles).filter((key) => {
    const file = files.find((f) => `${f.source}/${f.filename}` === key);
    return file && !file.parsed;
  }).length;

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      {/* Header */}
      <div className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Yields Admin</h1>
          <p className="text-sm text-gray-400">
            RMA Data Management
            {lastScan && (
              <span> &middot; Last scan: {formatDate(lastScan)}</span>
            )}
          </p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={handleScan}
            disabled={scanning}
            className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-500 disabled:opacity-50"
          >
            {scanning ? 'Scanning RMA...' : 'Check for Updates'}
          </button>
          <button
            onClick={handleLogout}
            className="rounded-lg border border-gray-700 px-4 py-2 text-sm font-medium text-gray-300 hover:bg-gray-800"
          >
            Logout
          </button>
        </div>
      </div>

      {/* ============================================================ */}
      {/* SECTION 1: RMA Remote Files (Scan & Download) */}
      {/* ============================================================ */}
      <div className="mb-10">
        <h2 className="mb-4 text-lg font-semibold text-white">RMA Remote Files</h2>

        {/* Scan Summary Banner */}
        {scanSummary && (
          <div className="mb-4 rounded-lg border border-gray-800 bg-gray-900 p-4">
            <h3 className="mb-2 text-sm font-medium text-gray-300">
              Scan completed at {formatDate(scanSummary.scannedAt)}
            </h3>
            <div className="flex flex-wrap gap-4">
              {scanSummary.sources.map((s) => (
                <div key={s.id} className="text-sm">
                  <span className="font-medium text-white">{s.label}:</span>{' '}
                  {s.newCount > 0 && (
                    <span className="text-green-400">{s.newCount} new</span>
                  )}
                  {s.newCount > 0 && s.updatedCount > 0 && ', '}
                  {s.updatedCount > 0 && (
                    <span className="text-amber-400">{s.updatedCount} updated</span>
                  )}
                  {s.newCount === 0 && s.updatedCount === 0 && (
                    <span className="text-gray-500">up to date</span>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Source Tabs */}
        <div className="mb-4 flex gap-1 rounded-lg border border-gray-800 bg-gray-900 p-1">
          {SOURCE_TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex-1 rounded-md px-4 py-2 text-sm font-medium transition-colors ${
                activeTab === tab.id
                  ? 'bg-gray-700 text-white'
                  : 'text-gray-400 hover:text-gray-200'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* New/Updated Files from Scan */}
        {activeScan && newAndUpdated.length > 0 && (
          <div className="rounded-lg border border-green-900/50 bg-green-950/30 p-4">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-sm font-medium text-green-400">
                {newAndUpdated.length} file{newAndUpdated.length !== 1 ? 's' : ''} available
              </h3>
              <button
                onClick={() => handleDownloadAll(newAndUpdated.filter((f) => !downloadedFilenames.has(f.filename)))}
                disabled={downloadingAll}
                className="rounded bg-green-700 px-3 py-1 text-xs font-medium text-white hover:bg-green-600 disabled:opacity-50"
              >
                {downloadingAll ? 'Downloading...' : 'Download All New'}
              </button>
            </div>
            <div className="max-h-96 space-y-2 overflow-y-auto">
              {newAndUpdated.map((file) => {
                const alreadyDownloaded = downloadedFilenames.has(file.filename);
                return (
                  <div
                    key={file.filename}
                    className="flex items-center justify-between rounded bg-gray-900/50 px-3 py-2 text-sm"
                  >
                    <div className="flex items-center gap-3">
                      <span className="font-mono text-gray-200">{file.filename}</span>
                      <span className="text-gray-500">{formatBytes(file.remoteSize)}</span>
                      <span className="text-gray-500">{formatDate(file.remoteDate)}</span>
                    </div>
                    {alreadyDownloaded ? (
                      <span className="rounded bg-amber-900/50 px-2 py-1 text-xs text-amber-400">
                        Already Downloaded
                      </span>
                    ) : (
                      <button
                        onClick={() => handleDownload(file)}
                        disabled={downloading.has(file.filename)}
                        className="rounded bg-blue-700 px-3 py-1 text-xs font-medium text-white hover:bg-blue-600 disabled:opacity-50"
                      >
                        {downloading.has(file.filename) ? 'Downloading...' : 'Download'}
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {activeScan && newAndUpdated.length === 0 && (
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-4 text-center text-sm text-gray-400">
            All files up to date ({activeScan.unchangedCount} files checked)
          </div>
        )}

        {activeScan && activeScan.errors.length > 0 && (
          <div className="mt-4 rounded-lg border border-red-900/50 bg-red-950/30 p-4">
            <h3 className="mb-2 text-sm font-medium text-red-400">Scan Errors</h3>
            {activeScan.errors.map((err, i) => (
              <p key={i} className="text-xs text-red-300">{err}</p>
            ))}
          </div>
        )}

        {!scanResults && (
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-8 text-center text-sm text-gray-500">
            Click &quot;Check for Updates&quot; to scan RMA for available files
          </div>
        )}
      </div>

      {/* ============================================================ */}
      {/* SECTION 2: Local Files (Manage & Push to Supabase) */}
      {/* ============================================================ */}
      <div>
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-white">Local Files</h2>
          {selectedUnpushedCount > 0 && (
            <button
              onClick={handlePushToSupabase}
              disabled={pushing}
              className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
            >
              {pushing
                ? 'Pushing to Supabase...'
                : `Push ${selectedUnpushedCount} File${selectedUnpushedCount !== 1 ? 's' : ''} to Supabase`}
            </button>
          )}
        </div>

        {/* Local Source Tabs */}
        <div className="mb-4 flex gap-1 rounded-lg border border-gray-800 bg-gray-900 p-1">
          {SOURCE_TABS.map((tab) => {
            const count = files.filter((f) => f.source === tab.id).length;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveLocalTab(tab.id)}
                className={`flex-1 rounded-md px-4 py-2 text-sm font-medium transition-colors ${
                  activeLocalTab === tab.id
                    ? 'bg-gray-700 text-white'
                    : 'text-gray-400 hover:text-gray-200'
                }`}
              >
                {tab.label}
                <span className="ml-2 text-xs text-gray-500">({count})</span>
              </button>
            );
          })}
        </div>

        {/* Push Results Banner */}
        {pushResults && (
          <div className="mb-4 rounded-lg border border-emerald-900/50 bg-emerald-950/30 p-4">
            <h3 className="mb-2 text-sm font-medium text-emerald-400">Push Results</h3>
            <div className="space-y-1">
              {Object.entries(pushResults).map(([key, result]) => (
                <div key={key} className="flex items-center gap-3 text-xs">
                  <span className="font-mono text-gray-300">{key.split('/').pop()}</span>
                  {result.status === 'success' ? (
                    <span className="text-emerald-400">
                      {result.rowsUpserted} rows loaded
                    </span>
                  ) : result.status === 'skipped' ? (
                    <span className="text-gray-500">Already in Supabase</span>
                  ) : (
                    <span className="text-red-400">{result.error}</span>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Local Files Table */}
        <div className="rounded-lg border border-gray-800 bg-gray-900">
          {localSourceFiles.length === 0 ? (
            <div className="p-8 text-center text-sm text-gray-500">
              No {SOURCE_TABS.find((t) => t.id === activeLocalTab)?.label} files downloaded yet
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-800 text-left text-xs uppercase text-gray-500">
                    <th className="px-4 py-3">
                      <input
                        type="checkbox"
                        checked={localAllSelected}
                        onChange={toggleAllLocalFiles}
                        className="rounded border-gray-600 bg-gray-800"
                      />
                    </th>
                    <th className="px-4 py-3">Filename</th>
                    <th className="px-4 py-3">Size</th>
                    <th className="px-4 py-3">Remote Date</th>
                    <th className="px-4 py-3">Downloaded</th>
                    <th className="px-4 py-3">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {localSourceFiles.map((file) => {
                    const key = `${file.source}/${file.filename}`;
                    const isSelected = selectedFiles.has(key);
                    return (
                      <tr
                        key={file.filename}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          isSelected ? 'bg-gray-800/20' : ''
                        }`}
                      >
                        <td className="px-4 py-3">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => toggleFileSelection(key)}
                            className="rounded border-gray-600 bg-gray-800"
                          />
                        </td>
                        <td className="px-4 py-3 font-mono text-gray-200">
                          {file.filename}
                        </td>
                        <td className="px-4 py-3 text-gray-400">
                          {formatBytes(file.fileSizeBytes)}
                        </td>
                        <td className="px-4 py-3 text-gray-400">
                          {formatDate(file.remoteDate)}
                        </td>
                        <td className="px-4 py-3 text-gray-400">
                          {formatDate(file.downloadedAt)}
                        </td>
                        <td className="px-4 py-3">
                          {file.parsed ? (
                            <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs text-emerald-400">
                              In Supabase
                            </span>
                          ) : (
                            <span className="rounded bg-gray-800 px-2 py-0.5 text-xs text-gray-500">
                              Local Only
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* ============================================================ */}
      {/* SECTION 3: Load from Disk */}
      {/* ============================================================ */}
      <div className="mt-10">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-white">Load from Disk</h2>
          {selectedDiskFiles.size > 0 && (
            <button
              onClick={handleLoadFromDisk}
              disabled={loadingDisk}
              className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
            >
              {loadingDisk
                ? 'Loading to Supabase...'
                : `Load ${selectedDiskFiles.size} File${selectedDiskFiles.size !== 1 ? 's' : ''} to Supabase`}
            </button>
          )}
        </div>

        {/* Breadcrumb */}
        <div className="mb-4 flex items-center gap-2 text-sm">
          <button
            onClick={() => browseTo('')}
            className="text-blue-400 hover:text-blue-300"
          >
            data/
          </button>
          {browseDir && browseDir !== '/' && browseDir.split('/').filter(Boolean).map((part, i, arr) => {
            const pathUpTo = arr.slice(0, i + 1).join('/');
            return (
              <span key={pathUpTo} className="flex items-center gap-2">
                <span className="text-gray-600">/</span>
                <button
                  onClick={() => browseTo(pathUpTo)}
                  className="text-blue-400 hover:text-blue-300"
                >
                  {part}
                </button>
              </span>
            );
          })}
        </div>

        {/* Background Job Status */}
        {diskJobs.length > 0 && (
          <div className="mb-4 rounded-lg border border-emerald-900/50 bg-emerald-950/30 p-4">
            <h3 className="mb-2 text-sm font-medium text-emerald-400">Load Jobs</h3>
            <div className="max-h-60 space-y-2 overflow-y-auto">
              {diskJobs.map((job) => (
                <div key={job.id} className="flex items-center gap-3 text-xs">
                  <span className="font-mono text-gray-300">{job.file.split('/').pop()}</span>
                  {job.status === 'running' ? (
                    <span className="text-blue-400">
                      Loading... {job.rowsProcessed.toLocaleString()} processed / {job.rowsUpserted.toLocaleString()} upserted
                    </span>
                  ) : job.status === 'done' ? (
                    <span className="text-emerald-400">
                      {job.table} &mdash; {job.rowsUpserted.toLocaleString()} rows loaded
                    </span>
                  ) : (
                    <span className="text-red-400">
                      Error: {job.errors?.[0] || 'Unknown'} ({job.rowsUpserted.toLocaleString()} rows loaded before error)
                    </span>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* File Browser */}
        <div className="rounded-lg border border-gray-800 bg-gray-900">
          {browseLoading ? (
            <div className="p-8 text-center text-sm text-gray-500">Loading...</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-800 text-left text-xs uppercase text-gray-500">
                    <th className="px-4 py-3 w-10">
                      <input
                        type="checkbox"
                        checked={browseEntries.filter(e => e.type === 'file' && e.name.endsWith('.txt')).length > 0 &&
                          browseEntries.filter(e => e.type === 'file' && e.name.endsWith('.txt')).every(e => selectedDiskFiles.has(e.path))}
                        onChange={toggleAllDiskFiles}
                        className="rounded border-gray-600 bg-gray-800"
                      />
                    </th>
                    <th className="px-4 py-3">Name</th>
                    <th className="px-4 py-3">Record Type</th>
                    <th className="px-4 py-3">Size</th>
                    <th className="px-4 py-3">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {browseParent !== null && (
                    <tr
                      className="border-b border-gray-800/50 hover:bg-gray-800/30 cursor-pointer"
                      onClick={() => browseTo(browseParent || '')}
                    >
                      <td className="px-4 py-3"></td>
                      <td className="px-4 py-3 text-blue-400">..</td>
                      <td className="px-4 py-3"></td>
                      <td className="px-4 py-3"></td>
                      <td className="px-4 py-3"></td>
                    </tr>
                  )}
                  {browseEntries.map((entry) => {
                    const isTxt = entry.type === 'file' && entry.name.endsWith('.txt');
                    const isSupported = entry.recordType && SUPPORTED_TYPES.has(entry.recordType);
                    const isSelected = selectedDiskFiles.has(entry.path);

                    return (
                      <tr
                        key={entry.path}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          entry.type === 'directory' ? 'cursor-pointer' : ''
                        } ${isSelected ? 'bg-gray-800/20' : ''}`}
                        onClick={entry.type === 'directory' ? () => browseTo(entry.path) : undefined}
                      >
                        <td className="px-4 py-3">
                          {isTxt && (
                            <input
                              type="checkbox"
                              checked={isSelected}
                              onChange={(e) => { e.stopPropagation(); toggleDiskFile(entry.path); }}
                              className="rounded border-gray-600 bg-gray-800"
                            />
                          )}
                        </td>
                        <td className="px-4 py-3">
                          {entry.type === 'directory' ? (
                            <span className="text-blue-400">{entry.name}/</span>
                          ) : (
                            <span className="font-mono text-gray-200">{entry.name}</span>
                          )}
                        </td>
                        <td className="px-4 py-3">
                          {entry.recordType && (
                            <span className={`rounded px-2 py-0.5 text-xs ${
                              isSupported
                                ? 'bg-blue-900/50 text-blue-400'
                                : 'bg-gray-800 text-gray-500'
                            }`}>
                              {entry.recordType} {entry.recordName}
                            </span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-gray-400">
                          {entry.type === 'file' ? formatBytes(entry.size) : ''}
                        </td>
                        <td className="px-4 py-3">
                          {isTxt && isSupported && (
                            <span className="rounded bg-blue-900/30 px-2 py-0.5 text-xs text-blue-400">
                              Supported
                            </span>
                          )}
                          {isTxt && !isSupported && entry.recordType && (
                            <span className="rounded bg-gray-800 px-2 py-0.5 text-xs text-gray-500">
                              Not mapped
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                  {browseEntries.length === 0 && (
                    <tr>
                      <td colSpan={5} className="px-4 py-8 text-center text-gray-500">
                        Empty directory
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
