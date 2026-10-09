# Kestra Apache Iceberg Plugin

## What

- Provides Apache Iceberg catalog control-plane tasks under `io.kestra.plugin.iceberg` and `io.kestra.plugin.iceberg.catalog`.
- Enables managing namespaces and tables against Apache Iceberg REST catalogs directly from Kestra workflows.

## Why

- What user problem does this solve? Orchestrating modern data lakehouses requires managing table lifecycles, schema definitions, and partition layouts without introducing heavy runtime engines like Spark or Flink.
- Why would a team adopt this plugin in a workflow? It provides lightweight, declarative tasks for Iceberg REST catalog operations with credential redaction, Pebble expression templating, and idempotent execution.
- What operational/business outcome does it enable? Automates lakehouse DDL operations (namespace creation, table provisioning, schema definition, and table cleanup) as integral steps in data pipelines.

## How

### Architecture

Single-module plugin focused strictly on REST catalog control-plane operations. Source packages under `io.kestra.plugin`:

- `iceberg`: Base task abstraction (`AbstractIcebergTask`, `AbstractIcebergTableTask`) managing REST catalog lifecycle and credential redaction.
- `iceberg.catalog`: Catalog DDL tasks (`CreateNamespace`, `DropNamespace`, `ListNamespaces`, `CreateTable`, `DropTable`) and schema utilities (`IcebergSchemaUtils`).
- `iceberg.data`: Reserved for future data-plane tasks.
- `iceberg.maintenance`: Reserved for future maintenance tasks.

Infrastructure dependencies (Docker Compose services for CI integration testing via `docker-compose-ci.yml`):

- `rest`: Apache Iceberg REST catalog server fixture.
- `minio`: S3-compatible object storage backend.
- `mc`: MinIO client initializing the warehouse bucket.

### Key Plugin Classes

- `io.kestra.plugin.iceberg.AbstractIcebergTask`: Base task handling Iceberg REST catalog initialization, Pebble configuration rendering, and sensitive credential masking.
- `io.kestra.plugin.iceberg.AbstractIcebergTableTask`: Base task extending `AbstractIcebergTask` for table-level operations with namespace and table identifier resolution.
- `io.kestra.plugin.iceberg.catalog.CreateNamespace`: Creates an Iceberg namespace with optional metadata properties and `ifNotExists` guard.
- `io.kestra.plugin.iceberg.catalog.DropNamespace`: Drops an Iceberg namespace with optional `ifExists` and recursive `cascade` support.
- `io.kestra.plugin.iceberg.catalog.ListNamespaces`: Lists top-level or hierarchical child namespaces in the catalog.
- `io.kestra.plugin.iceberg.catalog.CreateTable`: Creates an Iceberg table using typed column definitions or raw JSON schema, with partition specs, properties, and custom storage location.
- `io.kestra.plugin.iceberg.catalog.DropTable`: Drops an Iceberg table with optional file purging (`purge`) and `ifExists` guard.
- `io.kestra.plugin.iceberg.catalog.IcebergSchemaUtils`: Parser and builder utility for Iceberg Schema and PartitionSpec definitions from user configurations.

### Project Structure

```
plugin-iceberg/
├── .github/
│   └── setup-unit.sh
├── src/main/java/io/kestra/plugin/iceberg/
│   ├── AbstractIcebergTask.java
│   ├── AbstractIcebergTableTask.java
│   ├── catalog/
│   │   ├── CreateNamespace.java
│   │   ├── CreateTable.java
│   │   ├── DropNamespace.java
│   │   ├── DropTable.java
│   │   ├── IcebergSchemaUtils.java
│   │   └── ListNamespaces.java
│   ├── data/
│   └── maintenance/
├── src/test/java/io/kestra/plugin/iceberg/
│   ├── AbstractIcebergTaskTest.java
│   ├── RestCatalogConnectivityTest.java
│   └── catalog/
│       ├── CatalogTasksTest.java
│       ├── IcebergSchemaUtilsTest.java
│       └── SharedInMemoryCatalog.java
├── docker-compose-ci.yml
├── build.gradle
└── README.md
```

## Local rules

- Maintain strictly REST-only catalog scope. Avoid introducing Hive, Nessie, Spark, Flink, or engine-specific runtime dependencies.
- Ensure all sensitive catalog credentials are protected with `@ToString.Exclude` and redacted in logs.
- Annotate task properties with appropriate `@PluginProperty(group = "...")` groups (`main`, `connection`, `advanced`, `reliability`).
- Run Gradle builds with `--no-watch-fs` on Windows environments to prevent file locking.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
- https://iceberg.apache.org/docs/latest/rest-catalog/
