# ratgtex-pipeline
Loads RatGTEx external IDs into RGD by mapping active rat genes to their Ensembl IDs and creating corresponding entries (XDB_KEY=148) in the RGD_ACC_XDB table.
Uses delta-based reconciliation: inserts new IDs, deletes obsolete ones, and updates modification dates for existing matches.
