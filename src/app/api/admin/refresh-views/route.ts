import { NextResponse } from 'next/server';
import { supabase } from '@/lib/db/supabase';
import { refreshCarrackYieldsViews } from '@/lib/db/refreshViews';
import { setViewRefreshState } from '@/lib/rma/loadStatus';

/**
 * Manual recovery endpoint — refreshes the CarrackYields materialized
 * views on demand. Useful when an automated refresh after parse/load
 * failed and we need to bring the public-facing app back to a fresh
 * state without re-running the whole ingest.
 *
 * Auth: gated by /src/middleware.ts which requires a valid admin session
 * cookie on every /api/admin/* path except /api/admin/auth/*.
 *
 * Also publishes to the shared ViewRefreshState so a dashboard polling
 * /api/admin/load/status will see "Refreshing views..." during the call.
 */
export async function POST() {
  setViewRefreshState({ status: 'running', startedAt: new Date().toISOString() });
  const result = await refreshCarrackYieldsViews(supabase);
  setViewRefreshState({
    status: result.success ? 'done' : 'error',
    completedAt: new Date().toISOString(),
    result,
  });
  return NextResponse.json(result, { status: result.success ? 200 : 500 });
}
