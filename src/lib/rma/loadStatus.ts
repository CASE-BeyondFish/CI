// Simple in-memory job tracker for background file loading
// Uses globalThis to survive Next.js dev mode hot reloads

import type { RefreshResult } from '@/lib/db/refreshViews';

export interface LoadJob {
  id: string;
  file: string;
  status: 'running' | 'done' | 'error';
  recordType: string | null;
  table: string | null;
  rowsProcessed: number;
  rowsUpserted: number;
  errors: string[];
  startedAt: string;
  completedAt: string | null;
}

/**
 * Shared view-refresh state across the whole admin process.
 * After a load batch finishes, if any job touched insurance_offers we
 * trigger a CarrackYields MV refresh — this state lets the dashboard
 * briefly show "Refreshing views..." without us having to attach the
 * substate to each individual LoadJob (the refresh is a single shared
 * event that follows the batch, not a per-job thing).
 */
export type ViewRefreshState =
  | { status: 'idle' }
  | { status: 'running'; startedAt: string }
  | { status: 'done'; completedAt: string; result: RefreshResult }
  | { status: 'error'; completedAt: string; result: RefreshResult };

const globalJobs = globalThis as typeof globalThis & {
  __loadJobs?: Map<string, LoadJob>;
  __viewRefreshState?: ViewRefreshState;
};
if (!globalJobs.__loadJobs) {
  globalJobs.__loadJobs = new Map<string, LoadJob>();
}
if (!globalJobs.__viewRefreshState) {
  globalJobs.__viewRefreshState = { status: 'idle' };
}
const jobs = globalJobs.__loadJobs;

export function createJob(id: string, file: string): LoadJob {
  const job: LoadJob = {
    id,
    file,
    status: 'running',
    recordType: null,
    table: null,
    rowsProcessed: 0,
    rowsUpserted: 0,
    errors: [],
    startedAt: new Date().toISOString(),
    completedAt: null,
  };
  jobs.set(id, job);
  return job;
}

export function getJob(id: string): LoadJob | undefined {
  return jobs.get(id);
}

export function getAllJobs(): LoadJob[] {
  return Array.from(jobs.values()).sort(
    (a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime()
  );
}

export function updateJob(id: string, updates: Partial<LoadJob>): void {
  const job = jobs.get(id);
  if (job) {
    Object.assign(job, updates);
  }
}

export function getViewRefreshState(): ViewRefreshState {
  return globalJobs.__viewRefreshState ?? { status: 'idle' };
}

export function setViewRefreshState(state: ViewRefreshState): void {
  globalJobs.__viewRefreshState = state;
}
