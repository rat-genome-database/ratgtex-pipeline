# ratgtex-pipeline

Loads RatGTEx external IDs into RGD by mapping active rat genes to their Ensembl IDs and
creating corresponding entries (XDB_KEY=148) in the `RGD_ACC_XDB` table.

## Overview

For each active rat gene, retrieves its associated Ensembl XDB IDs and creates corresponding
RatGTEx XDB ID entries. Syncs these entries against the database using delta-based reconciliation.

## Logic

1. **Load existing** — retrieve current RatGTEx XDB IDs from RGD for rat
2. **Compute incoming** — for each active rat gene, fetch its Ensembl gene IDs and
   build RatGTEx XDB ID entries using the Ensembl accession
3. **Insert** new entries not yet in RGD
4. **Delete** entries no longer supported by active genes
5. **Update** modification date on matching entries

## Logging

- `status` — pipeline progress and summary counters

## Build and run

Requires Java 17. Built with Gradle:
```
./gradlew clean assembleDist
```
