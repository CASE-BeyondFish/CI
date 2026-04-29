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
-- Operator usage:
--   SELECT public.refresh_carrackyields_mvs();
-- Run this in the Supabase SQL editor after any ingest that wrote new rows
-- to insurance_offers — otherwise CarrackYields will show stale filter
-- options and county shading. Takes ~22 seconds at current data volumes
-- (the coverage MV is the heavy one).
--
-- Timeouts: statement_timeout and lock_timeout are pinned to 60s inside
-- the function body so a stuck refresh can't hang indefinitely or wait
-- forever on a lock. 60s is comfortably above the observed ~22s steady
-- state; raise if MV cost grows.
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
  -- Cap how long a stuck refresh / lock wait can hang. Function-scoped
  -- via SET LOCAL — reverts when the function returns.
  SET LOCAL statement_timeout = '60s';
  SET LOCAL lock_timeout      = '60s';

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

-- Lock down execution: revoke from PUBLIC, grant to service_role.
-- Current usage is operator-driven via the SQL editor (which runs as
-- postgres and bypasses the GRANT entirely). The service_role grant is
-- defense-in-depth in case a future caller invokes via supabase.rpc().
REVOKE ALL    ON FUNCTION public.refresh_carrackyields_mvs() FROM PUBLIC;
GRANT  EXECUTE ON FUNCTION public.refresh_carrackyields_mvs() TO service_role;
