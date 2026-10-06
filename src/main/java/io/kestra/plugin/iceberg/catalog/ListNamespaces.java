package io.kestra.plugin.iceberg.catalog;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.iceberg.AbstractIcebergTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Apache Iceberg namespaces",
    description = "Lists child namespaces under a parent namespace, or top-level namespaces if none specified."
)
@Plugin(
    examples = {
        @Example(
            title = "List all top-level namespaces in an Iceberg REST catalog",
            full = true,
            code = """
                id: list_iceberg_namespaces
                namespace: company.team

                tasks:
                  - id: list_namespaces
                    type: io.kestra.plugin.iceberg.catalog.ListNamespaces
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                """
        ),
        @Example(
            title = "List child namespaces under a specific parent namespace",
            full = true,
            code = """
                id: list_child_namespaces
                namespace: company.team

                tasks:
                  - id: list_namespaces
                    type: io.kestra.plugin.iceberg.catalog.ListNamespaces
                    catalogConfig:
                      type: rest
                      uri: "https://iceberg-catalog.example.com:8181"
                      credential: "{{ secret('ICEBERG_CREDENTIAL') }}"
                    namespace: "analytics"
                """
        )
    }
)
public class ListNamespaces extends AbstractIcebergTask implements RunnableTask<ListNamespaces.Output> {

    @Schema(
        title = "Parent namespace",
        description = "Optional parent namespace to list child namespaces under. If omitted or empty, lists top-level namespaces."
    )
    private Property<String> namespace;

    @Override
    public ListNamespaces.Output run(RunContext runContext) throws Exception {
        Namespace parentNs = Namespace.empty();
        if (namespace != null) {
            String rendered = runContext.render(namespace).as(String.class).orElse(null);
            if (rendered != null && !rendered.trim().isEmpty()) {
                parentNs = parseNamespace(rendered);
            }
        }

        Catalog catalog = catalog(runContext);
        try {
            SupportsNamespaces namespaceCatalog = asSupportsNamespaces(catalog);

            List<Namespace> namespaces = parentNs.isEmpty()
                ? namespaceCatalog.listNamespaces()
                : namespaceCatalog.listNamespaces(parentNs);

            List<String> result = namespaces != null
                ? namespaces.stream().map(Namespace::toString).sorted().collect(Collectors.toList())
                : Collections.emptyList();

            runContext.logger().info("Found {} namespaces under '{}'", result.size(), parentNs);

            return Output.builder()
                .namespaces(result)
                .build();
        } finally {
            closeCatalog(catalog, runContext.logger());
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Namespaces",
            description = "List of namespace strings found in the catalog."
        )
        private final List<String> namespaces;
    }
}
