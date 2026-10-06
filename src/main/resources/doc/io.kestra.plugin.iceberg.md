The Apache Iceberg plugin enables Kestra to manage Apache Iceberg catalogs, ingest data directly from internal Kestra storage into Iceberg tables, and automate data lake maintenance operations.

## Subpackages

- `io.kestra.plugin.iceberg.catalog`: Control-plane tasks for managing Iceberg namespaces and tables (`CreateNamespace`, `DropNamespace`, `ListNamespaces`, `CreateTable`, `DropTable`).
- `io.kestra.plugin.iceberg.data`: Data-plane tasks for streaming and appending data into Iceberg tables.
- `io.kestra.plugin.iceberg.maintenance`: Maintenance tasks for snapshot expiration, orphan file cleanup, and data file optimization.

## Catalogs

This release supports the **REST Catalog** (`type: rest`). Configure the catalog via the `catalogConfig` property:

```yaml
catalogConfig:
  type: rest
  uri: "https://your-iceberg-rest-catalog:8181"
  credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
  warehouse: my-warehouse
```

Support for additional catalog backends (AWS Glue, Hive Metastore, Nessie) is planned for future releases.
