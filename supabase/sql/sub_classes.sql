-- ============================================================
-- sub_classes — A00530 lookup table
--
-- Resolves sub_class_code (raw values like "003", "025") to human
-- names ("Desi", "Small Kabuli"). Used by CarrackYields to render
-- the sub-class dimension in panel readouts and dropdowns where it
-- currently shows raw codes.
--
-- Source: 2026_A00530_SubClass_YTD.txt (RMA ADM YTD release).
-- Despite the brief's prediction that this file would carry
-- (commodity_code, class_code, sub_class_code) tuples, the actual file
-- is a flat code -> name table — sub-class names are global, not
-- nested under a commodity/class. Natural key is just
-- (reinsurance_year, sub_class_code).
--
-- This migration is idempotent (CREATE TABLE / INDEX IF NOT EXISTS) —
-- safe to re-run any time.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.sub_classes (
  id                     bigserial   PRIMARY KEY,
  reinsurance_year       smallint    NOT NULL,
  sub_class_code         varchar(3)  NOT NULL,
  sub_class_name         varchar(100),
  sub_class_abbreviation varchar(20)
);

CREATE UNIQUE INDEX IF NOT EXISTS sub_classes_natural_key
  ON public.sub_classes (reinsurance_year, sub_class_code);
