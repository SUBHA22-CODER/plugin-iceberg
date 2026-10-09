package io.kestra.plugin.iceberg;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Arrays;

/**
 * Shared base task for Apache Iceberg table-level tasks.
 * Manages namespace and table name resolution, table identifiers, and table loading.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractIcebergTableTask extends AbstractIcebergTask {

    @Schema(
        title = "Table namespace",
        description = "The namespace or database of the Iceberg table (e.g. 'analytics' or multi-level 'analytics.raw')"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> namespace;

    @Schema(
        title = "Table name",
        description = "The name of the Iceberg table"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> tableName;

    /**
     * Resolves the TableIdentifier by rendering namespace and tableName expressions.
     * Supports dot-separated hierarchical namespaces (e.g. 'analytics.raw' -> Namespace.of("analytics", "raw")).
     *
     * @param runContext The current Kestra RunContext
     * @return The Iceberg TableIdentifier
     */
    protected TableIdentifier tableIdentifier(RunContext runContext) throws Exception {
        String renderedNs = runContext.render(namespace).as(String.class).orElseThrow(
            () -> new IllegalArgumentException("'namespace' must not be null or empty.")
        );
        String renderedTable = runContext.render(tableName).as(String.class).orElseThrow(
            () -> new IllegalArgumentException("'tableName' must not be null or empty.")
        );

        Namespace ns = parseNamespace(renderedNs);
        if (ns.isEmpty()) {
            throw new IllegalArgumentException("Invalid namespace: '" + renderedNs + "'. Must contain at least one non-empty level.");
        }

        return TableIdentifier.of(ns, renderedTable.trim());
    }

    /**
     * Loads the target Iceberg Table from the initialized catalog.
     *
     * @param runContext The current Kestra RunContext
     * @param catalog    The initialized Iceberg Catalog
     * @return The loaded Iceberg Table instance
     */
    protected Table loadTable(RunContext runContext, Catalog catalog) throws Exception {
        TableIdentifier identifier = tableIdentifier(runContext);
        return catalog.loadTable(identifier);
    }
}
