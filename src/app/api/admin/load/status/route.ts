import { NextResponse } from 'next/server';
import { getAllJobs, getViewRefreshState } from '@/lib/rma/loadStatus';

export async function GET() {
  const jobs = getAllJobs();
  const viewRefresh = getViewRefreshState();
  return NextResponse.json({ jobs, viewRefresh });
}
