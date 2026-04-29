// Pure path-parsing helper for SPOI PDFs.
// Kept in its own module so it can be unit-tested in isolation
// (see scripts/test_spoi_parse_filename.mjs) without importing the
// full ingest script's Supabase / fs side effects.

/**
 * @typedef {Object} ParsedSpoi
 * @property {number} year                 4-digit reinsurance year
 * @property {string} state_code           FIPS state, zero-padded (as-found in filename)
 * @property {string} county_code          FIPS county, zero-padded (as-found in filename)
 * @property {string} insurance_plan_code  Insurance plan code (as-found)
 * @property {string} commodity_code       Commodity code (as-found, typically 4 digits)
 * @property {string} filing_date          ISO date YYYY-MM-DD
 */

/**
 * Parse an SPOI PDF path into its component fields.
 *
 * Expected layout (any leading prefix tolerated):
 *   .../special_provisions/<year>/<YYYYMMDD>/<plan>/<commodity>/<state>_<county>_<plan>_<commodity>_<YYYYMMDD>.pdf
 *
 * The function cross-checks the filename's encoded plan / commodity /
 * filing-date against the directory structure — if they disagree, the
 * file is rejected (returns null) so we don't silently catalog a
 * misnamed PDF.
 *
 * Path separators are normalized; both forward and backslash inputs work.
 *
 * @param {string} absolutePath
 * @returns {ParsedSpoi | null}
 */
export function parseSpoiPath(absolutePath) {
  if (typeof absolutePath !== 'string' || !absolutePath) return null;

  const normalized = absolutePath.replace(/\\/g, '/');

  // Dir structure: .../special_provisions/<year>/<YYYYMMDD>/<plan>/<commodity>/<filename>
  const dirMatch = normalized.match(
    /special_provisions\/(\d{4})\/(\d{8})\/([^/]+)\/([^/]+)\/([^/]+\.pdf)$/i
  );
  if (!dirMatch) return null;
  const [, yearStr, filingStr, planDir, commodityDir, filename] = dirMatch;

  // Filename: <state>_<county>_<plan>_<commodity>_<YYYYMMDD>.pdf
  const fileMatch = filename.match(/^(\d+)_(\d+)_(\d+)_(\d+)_(\d{8})\.pdf$/i);
  if (!fileMatch) return null;
  const [, state, county, planFromName, commodityFromName, filingFromName] = fileMatch;

  // Cross-check: dir-encoded fields must match filename-encoded fields.
  if (
    planFromName !== planDir ||
    commodityFromName !== commodityDir ||
    filingFromName !== filingStr
  ) {
    return null;
  }

  // Validate the date is real (rejects e.g. 20251301).
  const yyyy = filingStr.slice(0, 4);
  const mm = filingStr.slice(4, 6);
  const dd = filingStr.slice(6, 8);
  const filing_date = `${yyyy}-${mm}-${dd}`;
  const parsed = new Date(filing_date + 'T00:00:00Z');
  if (
    Number.isNaN(parsed.getTime()) ||
    parsed.getUTCFullYear() !== Number(yyyy) ||
    parsed.getUTCMonth() + 1 !== Number(mm) ||
    parsed.getUTCDate() !== Number(dd)
  ) {
    return null;
  }

  return {
    year: Number(yearStr),
    state_code: state,
    county_code: county,
    insurance_plan_code: planFromName,
    commodity_code: commodityFromName,
    filing_date,
  };
}

/**
 * Compute the natural-key string used by the ingest script's dedup map
 * AND by the catalog table's unique index. Stable across runs.
 *
 * @param {Pick<ParsedSpoi,'year'|'state_code'|'county_code'|'insurance_plan_code'|'commodity_code'>} p
 * @returns {string}
 */
export function naturalKey(p) {
  return `${p.year}|${p.state_code}|${p.county_code}|${p.insurance_plan_code}|${p.commodity_code}`;
}

/**
 * Storage path inside the spoi-documents Storage bucket. Does NOT include
 * filing_date — overwriting the slot on each new filing is the whole
 * point of "latest filing wins."
 *
 * @param {Pick<ParsedSpoi,'year'|'state_code'|'county_code'|'insurance_plan_code'|'commodity_code'>} p
 * @returns {string}
 */
export function storagePath(p) {
  return `${p.year}/${p.state_code}_${p.county_code}_${p.insurance_plan_code}_${p.commodity_code}.pdf`;
}
