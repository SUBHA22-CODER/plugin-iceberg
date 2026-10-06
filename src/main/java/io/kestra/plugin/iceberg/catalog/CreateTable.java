package io.kestra.plugin.iceberg.catalog;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.iceberg.AbstractIcebergTableTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an Apache Iceberg table",
    description = "Creates a new Apache Iceberg table in the catalog with column definitions or raw schema JSON, partition specs, and properties."
)
@Plugin(
    examples = {
        @Example(
            title = "Create an Iceberg table with column definitions and partitioning",
            full = true,
            code = """
                id: create_iceberg_table
                namespace: company.team

                tasks:
                  - id: create_table
                    type: io.kestra.plugin.iceberg.catalog.CreateTable
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics"
                    tableName: "events"
                    columns:
                      - name: id
                        type: long
                        required: true
                        doc: "Unique event identifier"
                      - name: event_time
                        type: timestamptz
                        required: true
                      - name: region
                        type: string
                      - name: payload
                        type: string
                    partitionFields:
                      - "days(event_time)"
                      - "identity(region)"
                    tableProperties:
                      write.format.default: parquet
                    ifNotExists: true
                """
        ),
        @Example(
            title = "Create an Iceberg table using raw schema JSON",
            full = true,
            code = """
                id: create_iceberg_table_json
                namespace: company.team

                tasks:
                  - id: create_table
                    type: io.kestra.plugin.iceberg.catalog.CreateTable
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics"
                    tableName: "raw_events"
                    schemaJson: |
                      {
                        "type": "struct",
                        "fields": [
                          {"id": 1, "name": "id", "required": true, "type": "long"},
                          {"id": 2, "name": "payload", "required": false, "type": "string"}
                        ]
                      }
                """
        )
    }
)
public class CreateTable extends AbstractIcebergTableTask implements RunnableTask<CreateTable.Output> {

    @Schema(
        title = "Columns",
        description = "List of column definitions specifying name, type, and optional required/doc flags."
    )
    private List<Column> columns;

    @Schema(
        title = "Schema JSON",
        description = "Raw Iceberg schema in JSON format. Takes precedence over 'columns' when specified."
    )
    private Property<String> schemaJson;

    @Schema(
        title = "Partition fields",
        description = "Optional list of column names or transforms for partitioning (e.g. 'region', 'days(event_time)', 'bucket(16, id)')."
    )
    private Property<List<String>> partitionFields;

    @Schema(
        title = "Table properties",
        description = "Optional table configuration properties (e.g. 'write.format.default: parquet')."
    )
    private Property<Map<String, String>> tableProperties;

    @Schema(
        title = "Table location",
        description = "Optional custom storage location (URI) for table data and metadata."
    )
    private Property<String> location;

    @Schema(
        title = "If not exists",
        description = "If true, do not fail if the table already exists.",
        defaultValue = "true"
    )
    @Builder.Default
    private Property<Boolean> ifNotExists = Property.of(true);

    @Override
    public CreateTable.Output run(RunContext runContext) throws Exception {
        TableIdentifier identifier = tableIdentifier(runContext);
        boolean skipIfExists = runContext.render(ifNotExists).as(Boolean.class).orElse(true);

        String renderedJson = schemaJson != null
            ? runContext.render(schemaJson).as(String.class).orElse(null)
            : null;

        List<String> renderedPartitionFields = partitionFields != null
            ? runContext.render(partitionFields).asList(String.class)
            : Collections.emptyList();

        Map<String, String> renderedProps = tableProperties != null
            ? runContext.render(tableProperties).asMap(String.class, String.class)
            : Collections.emptyMap();

        String renderedLoc = location != null
            ? runContext.render(location).as(String.class).orElse(null)
            : null;

        Catalog catalog = catalog(runContext);
        try {
            if (catalog.tableExists(identifier)) {
                if (!skipIfExists) {
                    throw new AlreadyExistsException("Table already exists: " + identifier);
                }
                runContext.logger().info("Table '{}' already exists, skipping creation.", identifier);
                Table table = catalog.loadTable(identifier);
                return Output.builder()
                    .table(identifier.toString())
                    .created(false)
                    .location(table.location())
                    .build();
            }

            org.apache.iceberg.Schema schema = IcebergSchemaUtils.buildSchema(renderedJson, columns);
            PartitionSpec spec = IcebergSchemaUtils.buildPartitionSpec(schema, renderedPartitionFields);

            runContext.logger().info("Creating Iceberg table '{}'", identifier);
            Catalog.TableBuilder builder = catalog.buildTable(identifier, schema)
                .withPartitionSpec(spec)
                .withProperties(renderedProps != null ? renderedProps : Collections.emptyMap());

            if (renderedLoc != null && !renderedLoc.trim().isEmpty()) {
                builder.withLocation(renderedLoc.trim());
            }

            Table createdTable = builder.create();

            return Output.builder()
                .table(identifier.toString())
                .created(true)
                .location(createdTable.location())
                .build();
        } finally {
            closeCatalog(catalog, runContext.logger());
        }
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Column {
        @Schema(title = "Column name", description = "Name of the column")
        @NotNull
        private String name;

        @Schema(
            title = "Data type",
            description = "Iceberg data type: boolean, int/integer, long, float, double, date, time, timestamp, timestamptz, string, uuid, binary, decimal(p,s), fixed(l)"
        )
        @NotNull
        private String type;

        @Schema(title = "Required", description = "Whether the field is required (non-null)", defaultValue = "false")
        @Builder.Default
        private Boolean required = false;

        @Schema(title = "Documentation", description = "Optional column documentation or comment")
        private String doc;
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
            title = "Created",
            description = "Whether the table was created (false if it already existed)."
        )
        private final Boolean created;

        @Schema(
            title = "Table location",
            description = "The storage location of the table."
        )
        private final String location;
    }
}
