# `famcare-etl-governed`
Governed transformation pipeline for FAMCare extracts. Standardizes, cleans, and aligns data across programs to produce reporting‑ready, enrollment‑grain datasets for analytics and QA.

## Overview
`famcare-etl-governed` implements the governed transformation layer of the BHN FAMCare data ecosystem. It operates on flat files exported from FAMCare as `.csv` and produces consistent, auditable datasets. Pipeline execution and dependency tracking are managed by `{targets}`, and selected outputs are materialized as `.rds` files under `data_intermediate/etl/<program_short_code>/` (and `bhn_wide/`). The resulting datasets should be suitable for compiling written reports, infographic briefs, and program analytics. This ETL is intended to be **program-agnostic** with the ability to support multiple FAMCare-based BHN programs with minimal structural changes.

## Key Features
This ETL pipeline is built around:
- **Performing metadata-driven cleaning** using the `analytic_fields` table
- **Normalizating and standardizing** FAMCare extracts
- **Pivoting the Pathway event-grain program `pathclient_enrollments` tables** to produce program-specific enrollment-grain tibbles
- **Joining client demographics data** to the enrollment-grain tibble
- **Joining the Pathway Event form tables** to represent the entire referral flow from program enrollment to matriculation (when relevant) using `tiedenrollment`
- **Joining only the latest slowly changing dimension (SCD) records** using `parent_form.docserno` = `scd_form.parent_docserno`
- **Producing a final wide, reporting-ready full referral flow table**
- **Loading all SCD rows** to a standalone, long-format tibble
- **Loading all program enrollments** in `q-foo-providerplacement` to a standalone tibble
- **Saving transparent, auditable transformations** with intermediate diagnostics tibbles

## Architecture
This repository represents the **transformation and reporting** layer of the FAMCare ETL pipeline:

```text
FAMCare Extracts (view-based)
        ↓
famcare-etl-governed  ← (this repo)
  - Cleaning
  - Normalization
  - SCD logic
  - Pathway Event alignment and joins to program enrollments
  - Wide table construction
        ↓
Analytics-ready datasets (program_foo_full_data, subsets, diagnostics)
```

### Execution model

The pipeline is orchestrated with `{targets}`:

- **Dependency tracking:** Only targets whose inputs have changed are recomputed.
- **Storage:** Target results are cached in the `_targets` store.
- **Exported artifacts:** Program-level outputs are written as `.rds` files to `data_intermediate/etl/<program_short_code>/` (and `data_intermediate/etl/bhn_wide/`).
- **Consumption:** Parent projects (e.g., Quarto reports) call `tar_make()` / `tar_read()` or read the exported `.rds` files.


## Outputs
The ETL produces a variety of objects in a structured list for downstream use. This list includes:
- raw tibbles for each extract
- intermediate tibble `joined_pathclient`: an authoritative enrollment grain table with demographics
- intermediate tibble `joined_referral_flow`: the wide, fully joined Pathway Event table with active SCD records joined
- intermediate tibbles `intake_one`, `payor_one`, `housing_one`: tables of active SCD records for QA
- intermediate tibble `parent_map`: event `docserno` to SCD summation `parent_docserno` mapping for QA
- `program_foo_full_data`: the final, wide, enrollment-grained dataset
- optional subsets based on date ranges and fiscal system, typically constructed in parent projects via `build_subsets()` using the program's `*_full_data` table

## Status
This repository is part of the transition from legacy ETL to a modern, governed pipeline. The legacy workflow will remain active during the transition period.

## Future Work
Anticipated enhancements include:
- Support for additional FAMCare-based programs
- Optional future integration with vendor API (pending feasibility)
- Expanded metadata governance

## Contributing
Analysts are encouraged to contribute improvements, documentation, and program extensions. Please follow the established coding style (a modified version of tidyverse style), naming conventions, and metadata governance standards. All changes must be made in a feature branch and submitted via pull request. Direct commits to the `main` branch are disallowed.
