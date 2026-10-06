package io.kestra.plugin.iceberg.catalog;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.iceberg.AbstractIcebergTableTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Drop an Apache Iceberg table",
    description = "Drops an Apache Iceberg table from the catalog, optionally purging underlying data and metadata files."
)
@Plugin(
    examples = {
        @Example(
            title = "Drop an Iceberg table without purging underlying data files",
            full = true,
            code = """
                id: drop_iceberg_table
                namespace: company.team

                tasks:
                  - id: drop_table
                    type: io.kestra.plugin.iceberg.catalog.DropTable
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics"
                    tableName: "scratch_events"
                    purge: false
                    ifExists: true
                """
        ),
        @Example(
            title = "Drop an Iceberg table and completely purge all data and metadata files",
            full = true,
            code = """
                id: purge_iceberg_table
                namespace: company.team

                tasks:
                  - id: purge_table
                    type: io.kestra.plugin.iceberg.catalog.DropTable
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics"
                    tableName: "temp_table"
                    purge: true
                    ifExists: true
                """
        )
    }
)
public class DropTable extends AbstractIcebergTableTask implements RunnableTask<DropTable.Output> {

    @Schema(
        title = "Purge data",
        description = "If true, completely deletes underlying data and metadata files instead of just removing catalog metadata.",
        defaultValue = "false"
    )
    @Builder.Default
    private Property<Boolean> purge = Property.of(false);

    @Schema(
        title = "If exists",
        description = "If true, do not fail when the table does not exist.",
        defaultValue = "true"
    )
    @Builder.Default
    private Property<Boolean> ifExists = Property.of(true);

    @Override
    public DropTable.Output run(RunContext runContext) throws Exception {
        TableIdentifier identifier = tableIdentifier(runContext);
        boolean skipIfNotExists = runContext.render(ifExists).as(Boolean.class).orElse(true);
        boolean purgeData = runContext.render(purge).as(Boolean.class).orElse(false);

        Catalog catalog = catalog(runContext);
        try {
            if (!catalog.tableExists(identifier)) {
                if (!skipIfNotExists) {
                    throw new NoSuchTableException("Table does not exist: " + identifier);
                }
                runContext.logger().info("Table '{}' does not exist, skipping drop.", identifier);
                return Output.builder()
                    .table(identifier.toString())
                    .dropped(false)
                    .build();
            }

            runContext.logger().info("Dropping Iceberg table '{}' (purge={})", identifier, purgeData);
            boolean dropped = catalog.dropTable(identifier, purgeData);

            return Output.builder()
                .table(identifier.toString())
                .dropped(dropped)
                .build();
        } finally {
            closeCatalog(catalog, runContext.logger());
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Table identifier",
            description = "The full table identifier (namespace.table)."
        )
        private final String table;

        @Schema(
            title = "Dropped",
            description = "Whether the table was dropped (false if it did not exist)."
        )
        private final Boolean dropped;
    }
}
