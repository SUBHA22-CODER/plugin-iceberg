package io.kestra.plugin.iceberg;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractIcebergTask extends Task {

    // REST only for now: Glue, Hive, Nessie and Hadoop follow once each is validated end-to-end.
    public static final Set<String> SUPPORTED_TYPES = Set.of("rest");

    private static final Pattern SECRET_KEY_PATTERN = Pattern.compile(
        "(?i)(credential|token|secret|password|access[-_.]?key|session)"
    );

    @Schema(
        title = "Iceberg catalog configuration properties",
        description = """
            Key-value configuration map passed directly to the Iceberg catalog. \
            The 'type' key is required and must be set to 'rest'. \
            Use 'uri' to supply the REST catalog endpoint URL. \
            Sensitive values such as 'credential', 'token', and 'secret' are redacted from logs."""
    )
    @NotNull
    @PluginProperty(group = "connection")
    @ToString.Exclude
    protected Property<Map<String, String>> catalogConfig;

    /**
     * Redacts known sensitive credential keys from a configuration map for safe logging and error reporting.
     */
    public static Map<String, String> redact(Map<String, String> config) {
        if (config == null) {
            return Collections.emptyMap();
        }
        var safe = new LinkedHashMap<String, String>();
        for (var entry : config.entrySet()) {
            var key = entry.getKey();
            if (key != null && SECRET_KEY_PATTERN.matcher(key).find()) {
                safe.put(key, "******");
            } else {
                safe.put(key, entry.getValue());
            }
        }
        return safe;
    }

    public static void closeCatalog(Catalog catalog, Logger logger) {
        if (catalog instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (logger != null) {
                    logger.warn("Failed to close Iceberg catalog", e);
                }
            }
        }
    }

    public static Namespace parseNamespace(String namespaceStr) {
        if (namespaceStr == null || namespaceStr.trim().isEmpty()) {
            return Namespace.empty();
        }
        var parts = Arrays.stream(namespaceStr.split("\\."))
            .map(String::trim)
            .filter(part -> !part.isEmpty())
            .toArray(String[]::new);
        if (parts.length == 0) {
            return Namespace.empty();
        }
        return Namespace.of(parts);
    }

    public static SupportsNamespaces asSupportsNamespaces(Catalog catalog) {
        if (catalog instanceof SupportsNamespaces supportsNamespaces) {
            return supportsNamespaces;
        }
        throw new UnsupportedOperationException(
            "Catalog '" + (catalog != null ? catalog.name() : "null") + "' does not support namespace management (does not implement SupportsNamespaces)."
        );
    }

    protected Catalog catalog(RunContext runContext) throws Exception {
        var rCatalogConfig = runContext.render(catalogConfig).asMap(String.class, String.class);
        if (rCatalogConfig == null || rCatalogConfig.isEmpty()) {
            throw new IllegalArgumentException("'catalogConfig' must be provided with at least 'type' or 'catalog-impl'.");
        }

        var type = rCatalogConfig.get("type");
        var catalogImpl = rCatalogConfig.get("catalog-impl");

        if (type == null && catalogImpl == null) {
            throw new IllegalArgumentException(
                "'catalogConfig' must contain either 'type' (supported: " + SUPPORTED_TYPES + ") or 'catalog-impl'."
            );
        }

        if (type != null) {
            var normalizedType = type.trim().toLowerCase(Locale.ROOT);
            if (!SUPPORTED_TYPES.contains(normalizedType)) {
                throw new IllegalArgumentException(
                    "Unsupported catalog type '" + type + "'. Supported types are: " + SUPPORTED_TYPES + " (or supply 'catalog-impl')."
                );
            }
        }

        // FileIO reads s3.*/io-impl from the Hadoop Configuration, so all catalog properties are forwarded.
        var hadoopConf = new Configuration();
        for (var entry : rCatalogConfig.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                hadoopConf.set(entry.getKey(), entry.getValue());
            }
        }

        var catalog = CatalogUtil.buildIcebergCatalog("kestra", rCatalogConfig, hadoopConf);
        runContext.logger().debug("Initialized Iceberg catalog with configuration: {}", redact(rCatalogConfig));
        return catalog;
    }
}
