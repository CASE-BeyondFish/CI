-- ============================================================
-- handbooks — catalog of RMA handbook PDFs mirrored to Supabase
--
-- One row per logical handbook = (document_short, document_version).
-- The actual PDF lives in the `handbooks` Supabase Storage bucket;
-- storage_path is the path inside that bucket.
--
-- Ingest is one-shot via scripts/ingest_handbooks.mjs. The script
-- reads scripts/handbooks-manifest.json (Henry curates this by hand),
-- uploads each PDF to Storage at a deterministic path, and upserts a
-- row here.
--
-- Why a manifest instead of filename parsing (cf. SPOI):
-- Handbook metadata — full document name, FCIC number, year semantics,
-- supersession notes — doesn't fit cleanly in filenames. New handbooks
-- arrive rarely (a handful per year), so manual curation in a JSON
-- manifest is the right level of investment.
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.handbooks (
  id                bigserial   PRIMARY KEY,
  document_short    varchar     NOT NULL,  -- e.g. "CCIP-BP", "CIH", "GSH", "LAM"
  document_full     varchar     NOT NULL,  -- e.g. "Common Crop Insurance Policy Basic Provisions"
  fcic_number       varchar,               -- e.g. "FCIC-18010-1" (nullable; not all docs have one)
  document_version  varchar     NOT NULL,  -- e.g. "26-BR", "2026", "23-0011"
  reinsurance_year  smallint,              -- e.g. 2026 (nullable; not all docs are year-keyed)
  storage_path      varchar     NOT NULL,  -- e.g. "ccip-bp/26-BR/Basic-Provisions-26-BR.pdf"
  source_filename   varchar     NOT NULL,  -- original filename from local folder
  file_size_bytes   integer     NOT NULL,
  rma_source_url    varchar,               -- where this came from on rma.usda.gov (for reference)
  notes             text,                  -- optional human notes ("Replaces 25-BR; supersedes via OBBBA")
  uploaded_at       timestamptz NOT NULL DEFAULT now()
);

-- Natural key: one row per (document_short, version)
CREATE UNIQUE INDEX IF NOT EXISTS handbooks_natural_key
  ON public.handbooks (document_short, document_version);

-- Lookup helpers
CREATE INDEX IF NOT EXISTS handbooks_year ON public.handbooks (reinsurance_year);
CREATE INDEX IF NOT EXISTS handbooks_fcic ON public.handbooks (fcic_number);
