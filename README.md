This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## CarrackYields materialized view refresh

When you ingest new data into `insurance_offers`, run this in the Supabase SQL editor:

```sql
SELECT public.refresh_carrackyields_mvs();
```

Takes ~22 seconds. Otherwise CarrackYields will show stale filter options and stale county shading.

The function definition lives at `supabase/sql/refresh_carrackyields_mvs.sql` — re-apply that migration if the function isn't present in your Supabase project. Refresh is operator-driven on purpose: ingests are rare and intentional, so reactive in-app wiring is over-engineering.

This refresh is only needed after ingests that write to `insurance_offers` (any ADM load that includes `A00030` records). Special Provisions ingest (below) writes to a separate `spoi_documents` table and does NOT affect the MVs — no need to run the refresh after a SPOI-only run.

## Special Provisions ingest

Special Provisions of Insurance (SPOI) data is a tree of ~52,000 PDFs, one per (state, county, insurance_plan, commodity) policy slice, organized by filing date. Unlike ADM, this is not parsed into structured rows — we catalog the documents and mirror them to Supabase Storage so CarrackYields can serve them by URL.

### One-time setup

1. In **Supabase Storage**, create a bucket named `spoi-documents`:
   - Public bucket: **enabled** (CarrackYields needs to serve PDF URLs without auth)
   - File size limit: **10 MB** (samples are ~12-40 KB, ample headroom)
   - Allowed MIME types: `application/pdf`
2. In the **Supabase SQL editor**, run `supabase/sql/spoi_documents.sql` to create the catalog table and indexes.

Both steps are idempotent — safe to re-run if you're not sure they were applied.

### Running the ingest

Ingest is a one-shot CLI script (`scripts/ingest_spoi.mjs`), not part of the dashboard load workflow. It walks `data/special_provisions/`, dedups to "latest filing per tuple" (currently a no-op — RMA's filings are additive, not delta replacements), then uploads each PDF to the bucket and upserts a row in `spoi_documents`.

```bash
# Dry run first (parses + dedups, prints summary, no writes)
node scripts/ingest_spoi.mjs --dry-run

# Real run (default: all years, 4 parallel uploads)
node scripts/ingest_spoi.mjs

# Restrict to one year (useful for the small 2025/2027 sets)
node scripts/ingest_spoi.mjs --year=2026

# Bump parallelism if Supabase is keeping up (max 16)
node scripts/ingest_spoi.mjs --concurrency=8
```

Re-running is safe: storage uploads use upsert (overwrites the slot), DB writes upsert on the natural key. A full run of ~50K PDFs at the default concurrency takes roughly 30 minutes.

The script reads `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` from `.env.local` — the service-role key is needed because writes bypass the bucket's public-read-only policy.

### What lands where

- `public.spoi_documents` — one row per `(reinsurance_year, state_code, county_code, insurance_plan_code, commodity_code)`. Includes `filing_date`, `storage_path`, `file_size_bytes`, `source_filename`, `ingested_at`.
- `spoi-documents` Storage bucket — PDFs at path `{year}/{state}_{county}_{plan}_{commodity}.pdf`. No filing date in the path; latest filing overwrites the slot.

## Handbook Mirror

CarrackRMA mirrors RMA handbook PDFs (Basic Provisions, Crop Insurance Handbook, General Standards, Loss Adjustment Manual, etc.) to Supabase Storage so CarrackYields' Definitions Library can cite stable URLs that won't break when RMA reorganizes their site.

The catalog is `public.handbooks`; the PDFs live in the `handbooks` Storage bucket. Henry curates the set by hand via `scripts/handbooks-manifest.json`.

### One-time setup

1. In **Supabase Storage**, create a bucket named `handbooks`:
   - Public bucket: **enabled** (CarrackYields needs to serve PDF URLs without auth)
   - File size limit: **50 MB** (the largest current handbook is ~15 MB; this gives headroom)
   - Allowed MIME types: `application/pdf`
2. In the **Supabase SQL editor**, run `supabase/sql/handbooks.sql` to create the catalog table and indexes.

Both steps are idempotent — safe to re-run if you're not sure they were applied. Bucket creation is intentionally manual: Supabase Storage policy setup via SQL is fragile.

### Adding new handbooks

1. Download the PDF from [rma.usda.gov](https://www.rma.usda.gov/policy-procedure) into your local handbook folder (default: `R:\CarrackReferences\`; override with `HANDBOOKS_ROOT` env or `--root=...`).
2. Add an entry to `scripts/handbooks-manifest.json` describing the PDF — `filename`, `document_short`, `document_full`, `fcic_number`, `document_version`, `reinsurance_year`, `rma_source_url`, `notes`. The natural key is `(document_short, document_version)`.
3. Run the ingest script:

```bash
# Dry run first (validates manifest, checks files exist, no writes)
node scripts/ingest_handbooks.mjs --dry-run

# Real run (uploads + upserts catalog rows)
node scripts/ingest_handbooks.mjs

# Restrict to one document_short
node scripts/ingest_handbooks.mjs --filter=CIH

# Override the local handbooks root for this run
node scripts/ingest_handbooks.mjs --root=D:\handbooks
```

Re-running is safe: storage uploads use upsert (overwrites the slot) and DB writes upsert on the natural key. The script reads `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` from `.env.local`.

Validation runs before any writes. If a manifest entry references a missing file, has duplicate keys, or exceeds the 50 MB limit, the script aborts with a clear error and leaves everything untouched.

### Listing what's mirrored

`GET /api/admin/handbooks/list` (admin session required) returns the current catalog as JSON. Useful for sanity-checking what's mirrored without opening the Supabase dashboard. There is no editing UI — Henry edits the manifest file and re-runs the script.

### What lands where

- `public.handbooks` — one row per `(document_short, document_version)`. Includes `document_full`, `fcic_number`, `reinsurance_year`, `storage_path`, `source_filename`, `file_size_bytes`, `rma_source_url`, `notes`, `uploaded_at`.
- `handbooks` Storage bucket — PDFs at path `{document_short_lower}/{document_version}/{filename}` (e.g. `cih/2026/2026-18010-1-Crop-Insurance-Handbook.pdf`).

### Where handbooks come from

The canonical source for RMA handbooks is `https://www.rma.usda.gov/policy-procedure`. CarrackYields citations should keep both URLs alongside each other — the mirror exists to insulate from RMA URL churn, not to replace RMA's authority.

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
