-- ============================================================
-- spoi_documents — catalog of Special Provisions of Insurance PDFs
--
-- One row per logical SPOI policy = (year, state, county, plan,
-- commodity). The actual PDF lives in the spoi-documents Supabase
-- Storage bucket; storage_path is the path inside that bucket.
--
-- Ingest is one-shot via scripts/ingest_spoi.mjs. The script walks the
-- local data/special_provisions/ tree (a delta-style filing-date
-- directory layout), keeps only the latest filing per natural-key
-- tuple, uploads each survivor to Storage at a deterministic path, and
-- upserts a row here.
--
-- Latest-filing-wins semantics:
-- The unique index on the natural key OMITS filing_date on purpose. A
-- second ingest of a newer filing for the same tuple upserts and
-- overwrites filing_date + storage_path. This matches the brief's
-- "merge, don't append" rule.
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.spoi_documents (
  id                  bigserial   PRIMARY KEY,
  reinsurance_year    smallint    NOT NULL,
  state_code          varchar     NOT NULL,
  county_code         varchar     NOT NULL,
  insurance_plan_code varchar     NOT NULL,
  commodity_code      varchar     NOT NULL,
  filing_date         date        NOT NULL,
  storage_path        varchar     NOT NULL,
  file_size_bytes     integer,
  source_filename     varchar,
  ingested_at         timestamptz NOT NULL DEFAULT now()
);

-- Natural key — sans filing_date, so "latest filing wins" via upsert.
CREATE UNIQUE INDEX IF NOT EXISTS spoi_documents_natural_key
  ON public.spoi_documents (
    reinsurance_year,
    state_code,
    county_code,
    insurance_plan_code,
    commodity_code
  );

-- Geographic lookup (state + county for a given year)
CREATE INDEX IF NOT EXISTS spoi_documents_geo
  ON public.spoi_documents (reinsurance_year, state_code, county_code);

-- Commodity lookup (commodity for a given year)
CREATE INDEX IF NOT EXISTS spoi_documents_commodity
  ON public.spoi_documents (reinsurance_year, commodity_code);
