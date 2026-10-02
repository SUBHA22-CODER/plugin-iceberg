The Apache Iceberg plugin enables Kestra to manage Apache Iceberg catalogs, ingest data directly from internal Kestra storage into Iceberg tables, and automate data lake maintenance operations.

## Subpackages

- `io.kestra.plugin.iceberg.catalog`: Control-plane tasks for managing Iceberg namespaces and tables.
- `io.kestra.plugin.iceberg.data`: Data-plane tasks for streaming and appending data into Iceberg tables.
- `io.kestra.plugin.iceberg.maintenance`: Maintenance tasks for snapshot expiration, orphan file cleanup, and data file optimization.

## Catalogs

The plugin supports connecting to multiple Iceberg catalog implementations via `catalogConfig`:
- REST Catalog (`type: rest`)
- AWS Glue Catalog (`type: glue`)
- Hive Metastore (`type: hive`)
- Nessie Catalog (`type: nessie`)
