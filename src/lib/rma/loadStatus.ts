// Simple in-memory job tracker for background file loading
// Uses globalThis to survive Next.js dev mode hot reloads

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

const globalJobs = globalThis as typeof globalThis & { __loadJobs?: Map<string, LoadJob> };
if (!globalJobs.__loadJobs) {
  globalJobs.__loadJobs = new Map<string, LoadJob>();
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
