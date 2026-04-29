-- ============================================================
-- refresh_carrackyields_mvs()
--
-- Refreshes the two materialized views CarrackYields depends on:
--   public.insurance_offer_combinations  -- cascading filter sidebar
--   public.insurance_offer_coverage      -- map shading
--
-- Both MVs were created during CarrackYields phases 1 and 2 with unique
-- indexes, so REFRESH MATERIALIZED VIEW CONCURRENTLY works (no read
-- blocking). If you ever see "cannot refresh materialized view ...
-- concurrently" on either, that MV is missing its unique index — add
-- one before relying on this function.
--
-- Returns a jsonb object with success flag and per-MV durations in ms.
-- Server callers use this via supabase.rpc('refresh_carrackyields_mvs').
--
-- This migration is idempotent (CREATE OR REPLACE + REVOKE/GRANT) — safe
-- to re-run any time.
-- ============================================================

CREATE OR REPLACE FUNCTION public.refresh_carrackyields_mvs()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
  start_combinations    timestamptz;
  start_coverage        timestamptz;
  duration_combinations interval;
  duration_coverage     interval;
BEGIN
  start_combinations := clock_timestamp();
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.insurance_offer_combinations;
  duration_combinations := clock_timestamp() - start_combinations;

  start_coverage := clock_timestamp();
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.insurance_offer_coverage;
  duration_coverage := clock_timestamp() - start_coverage;

  RETURN jsonb_build_object(
    'success', true,
    'duration_combinations_ms', extract(milliseconds from duration_combinations),
    'duration_coverage_ms',     extract(milliseconds from duration_coverage)
  );
END;
$$;

-- Lock down execution: revoke from PUBLIC, grant only to the server's role.
-- service_role is what the Next.js API routes use via SUPABASE_SERVICE_KEY.
REVOKE ALL    ON FUNCTION public.refresh_carrackyields_mvs() FROM PUBLIC;
GRANT  EXECUTE ON FUNCTION public.refresh_carrackyields_mvs() TO service_role;
