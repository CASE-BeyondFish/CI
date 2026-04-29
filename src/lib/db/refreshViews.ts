import type { SupabaseClient } from '@supabase/supabase-js';

/**
 * Result of a refresh attempt. `success: false` means refresh failed —
 * `error` is populated and `durations` may be zeros. Errors are NEVER
 * thrown from refreshCarrackYieldsViews; ingest pipelines call this and
 * keep going regardless of outcome.
 */
export type RefreshResult = {
  success: boolean;
  durations: { combinations: number; coverage: number };
  error?: string;
};

type RpcReturn = {
  success?: boolean;
  duration_combinations_ms?: number | string;
  duration_coverage_ms?: number | string;
};

/**
 * Calls the public.refresh_carrackyields_mvs() Postgres function via Supabase
 * RPC, parses its jsonb return, and produces a RefreshResult.
 *
 * Refreshes the two materialized views CarrackYields depends on:
 *   - public.insurance_offer_combinations  (cascading filter sidebar)
 *   - public.insurance_offer_coverage      (map shading)
 *
 * Both MVs go stale after any ingest that writes to insurance_offers.
 * This helper is the single entry point for refreshing them.
 *
 * Safety: errors are caught and returned in the result object. A refresh
 * failure must NOT propagate up and fail the ingest — the brief is
 * explicit about this. Callers should log on failure but otherwise
 * proceed.
 */
export async function refreshCarrackYieldsViews(
  supabase: SupabaseClient
): Promise<RefreshResult> {
  const startedAtMs = Date.now();
  console.log('[refreshViews] starting MV refresh');

  try {
    const { data, error } = await supabase.rpc('refresh_carrackyields_mvs');

    if (error) {
      console.error(`[refreshViews] RPC error: ${error.message}`);
      return {
        success: false,
        durations: { combinations: 0, coverage: 0 },
        error: error.message,
      };
    }

    const payload = (data ?? {}) as RpcReturn;
    const combinations = Number(payload.duration_combinations_ms ?? 0);
    const coverage = Number(payload.duration_coverage_ms ?? 0);
    const totalMs = Date.now() - startedAtMs;

    console.log(
      `[refreshViews] done in ${totalMs}ms ` +
      `(combinations=${Math.round(combinations)}ms, coverage=${Math.round(coverage)}ms)`
    );

    return {
      success: true,
      durations: { combinations, coverage },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[refreshViews] exception: ${msg}`);
    return {
      success: false,
      durations: { combinations: 0, coverage: 0 },
      error: msg,
    };
  }
}
