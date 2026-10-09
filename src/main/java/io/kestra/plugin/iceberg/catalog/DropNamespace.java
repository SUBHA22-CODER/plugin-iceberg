package io.kestra.plugin.iceberg.catalog;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.iceberg.AbstractIcebergTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Drop an Apache Iceberg namespace",
    description = "Drops an existing namespace (database) from the specified Iceberg catalog."
)
@Plugin(
    examples = {
        @Example(
            title = "Drop an Iceberg namespace if it exists, cascading to any contained tables",
            full = true,
            code = """
                id: drop_iceberg_namespace
                namespace: company.team

                tasks:
                  - id: drop_namespace
                    type: io.kestra.plugin.iceberg.catalog.DropNamespace
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics.scratch"
                    ifExists: true
                    cascade: true
                """
        )
    }
)
public class DropNamespace extends AbstractIcebergTask implements RunnableTask<DropNamespace.Output> {

    @Schema(
        title = "Namespace name",
        description = "The namespace or database to drop (e.g. 'analytics' or multi-level 'analytics.scratch')."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> namespace;

    @Schema(
        title = "If exists",
        description = "If true, do not fail when the namespace does not exist.",
        defaultValue = "true"
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> ifExists = Property.of(true);

    @Schema(
        title = "Cascade drop",
        description = "If true, drops all tables within the namespace before dropping the namespace itself.",
        defaultValue = "false"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> cascade = Property.of(false);

    @Override
    public DropNamespace.Output run(RunContext runContext) throws Exception {
        String renderedNs = runContext.render(namespace).as(String.class).orElseThrow(
            () -> new IllegalArgumentException("'namespace' must not be null or empty.")
        );
        Namespace ns = parseNamespace(renderedNs);
        if (ns.isEmpty()) {
            throw new IllegalArgumentException("Invalid namespace: '" + renderedNs + "'. Must contain at least one non-empty level.");
        }

        boolean skipIfNotExists = runContext.render(ifExists).as(Boolean.class).orElse(true);
        boolean cascadeDrop = runContext.render(cascade).as(Boolean.class).orElse(false);

        Catalog catalog = catalog(runContext);
        try {
            SupportsNamespaces namespaceCatalog = asSupportsNamespaces(catalog);

            if (!namespaceCatalog.namespaceExists(ns)) {
                if (!skipIfNotExists) {
                    throw new NoSuchNamespaceException("Namespace does not exist: " + ns);
                }
                runContext.logger().info("Namespace '{}' does not exist, skipping drop.", ns);
                return Output.builder()
                    .namespace(ns.toString())
                    .dropped(false)
                    .build();
            }

            if (cascadeDrop) {
                List<TableIdentifier> tables = catalog.listTables(ns);
                for (TableIdentifier table : tables) {
                    runContext.logger().info("Cascade dropping table '{}'", table);
                    catalog.dropTable(table, false);
                }
            }

            runContext.logger().info("Dropping Iceberg namespace '{}'", ns);
            boolean dropped = namespaceCatalog.dropNamespace(ns);

            return Output.builder()
                .namespace(ns.toString())
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
            title = "Namespace",
            description = "The namespace that was evaluated."
        )
        private final String namespace;

        @Schema(
            title = "Dropped",
            description = "Whether the namespace was dropped (false if it did not exist)."
        )
        private final Boolean dropped;
    }
}
