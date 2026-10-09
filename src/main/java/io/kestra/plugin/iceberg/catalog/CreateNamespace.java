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
import org.apache.iceberg.exceptions.AlreadyExistsException;

import java.util.Collections;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an Apache Iceberg namespace",
    description = "Creates a new namespace (database) in the specified Iceberg catalog."
)
@Plugin(
    examples = {
        @Example(
            title = "Create an Iceberg namespace if it does not already exist",
            full = true,
            code = """
                id: create_iceberg_namespace
                namespace: company.team

                tasks:
                  - id: create_namespace
                    type: io.kestra.plugin.iceberg.catalog.CreateNamespace
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics.marketing"
                    ifNotExists: true
                """
        )
    }
)
public class CreateNamespace extends AbstractIcebergTask implements RunnableTask<CreateNamespace.Output> {

    @Schema(
        title = "Namespace name",
        description = "The namespace or database to create (e.g. 'analytics' or multi-level 'analytics.marketing')."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> namespace;

    @Schema(
        title = "Namespace properties",
        description = "Optional metadata key-value properties for the namespace (e.g. 'location', 'comment')."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> properties;

    @Schema(
        title = "If not exists",
        description = "If true, skips creation without failing when the namespace already exists.",
        defaultValue = "true"
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> ifNotExists = Property.of(true);

    @Override
    public CreateNamespace.Output run(RunContext runContext) throws Exception {
        String renderedNs = runContext.render(namespace).as(String.class).orElseThrow(
            () -> new IllegalArgumentException("'namespace' must not be null or empty.")
        );
        Namespace ns = parseNamespace(renderedNs);
        if (ns.isEmpty()) {
            throw new IllegalArgumentException("Invalid namespace: '" + renderedNs + "'. Must contain at least one non-empty level.");
        }

        Map<String, String> renderedProps = properties != null
            ? runContext.render(properties).asMap(String.class, String.class)
            : Collections.emptyMap();

        boolean skipIfExists = runContext.render(ifNotExists).as(Boolean.class).orElse(true);

        Catalog catalog = catalog(runContext);
        try {
            SupportsNamespaces namespaceCatalog = asSupportsNamespaces(catalog);

            if (namespaceCatalog.namespaceExists(ns)) {
                if (!skipIfExists) {
                    throw new AlreadyExistsException("Namespace already exists: " + ns);
                }
                runContext.logger().info("Namespace '{}' already exists, skipping creation.", ns);
                return Output.builder()
                    .namespace(ns.toString())
                    .created(false)
                    .properties(namespaceCatalog.loadNamespaceMetadata(ns))
                    .build();
            }

            runContext.logger().info("Creating Iceberg namespace '{}'", ns);
            namespaceCatalog.createNamespace(ns, renderedProps != null ? renderedProps : Collections.emptyMap());

            return Output.builder()
                .namespace(ns.toString())
                .created(true)
                .properties(renderedProps != null ? renderedProps : Collections.emptyMap())
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
            title = "Created",
            description = "Whether the namespace was created (false if it already existed)."
        )
        private final Boolean created;

        @Schema(
            title = "Properties",
            description = "The properties associated with the namespace."
        )
        private final Map<String, String> properties;
    }
}
