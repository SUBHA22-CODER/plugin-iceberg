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

/**
 * Shared base task for Apache Iceberg tasks.
 * Manages catalog configuration rendering, credential redaction, and catalog lifecycle.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractIcebergTask extends Task {

    /**
     * Catalog types supported in this release. Only the REST catalog is included
     * in Phase 1; additional types (Glue, Hive, Nessie, Hadoop) are planned for
     * future phases once each integration is validated end-to-end.
     */
    public static final Set<String> SUPPORTED_TYPES = Set.of("rest");

    private static final Pattern SECRET_KEY_PATTERN = Pattern.compile(
        "(?i)(credential|token|secret|password|access[-_.]?key|session)"
    );

    @Schema(
        title = "Iceberg catalog configuration properties",
        description = "Key-value configuration map passed directly to the Iceberg catalog. " +
            "The 'type' key is required and must be set to 'rest'. " +
            "Use 'uri' to supply the REST catalog endpoint URL. " +
            "Sensitive values such as 'credential', 'token', and 'secret' are redacted from logs."
    )
    @NotNull
    @PluginProperty(group = "connection")
    @ToString.Exclude
    protected Property<Map<String, String>> catalogConfig;

    /**
     * Redacts known sensitive credential keys from a configuration map for safe logging and error reporting.
     *
     * @param config The raw configuration map
     * @return A map with sensitive values replaced by '******'
     */
    public static Map<String, String> redact(Map<String, String> config) {
        if (config == null) {
            return Collections.emptyMap();
        }
        Map<String, String> safe = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (key != null && SECRET_KEY_PATTERN.matcher(key).find()) {
                safe.put(key, "******");
            } else {
                safe.put(key, entry.getValue());
            }
        }
        return safe;
    }

    /**
     * Closes the catalog safely if it implements AutoCloseable/Closeable to avoid resource and thread leaks.
     *
     * @param catalog The catalog instance to close
     * @param logger  Optional logger for non-fatal close warnings
     */
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

    /**
     * Parses a dot-separated string into an Iceberg Namespace.
     * Empty or null string returns an empty Namespace (root).
     *
     * @param namespaceStr The dot-separated namespace string (e.g. "analytics" or "analytics.raw")
     * @return The Iceberg Namespace
     */
    public static Namespace parseNamespace(String namespaceStr) {
        if (namespaceStr == null || namespaceStr.trim().isEmpty()) {
            return Namespace.empty();
        }
        String[] parts = Arrays.stream(namespaceStr.split("\\."))
            .map(String::trim)
            .filter(part -> !part.isEmpty())
            .toArray(String[]::new);
        if (parts.length == 0) {
            return Namespace.empty();
        }
        return Namespace.of(parts);
    }

    /**
     * Checks if the catalog implements SupportsNamespaces and casts it, throwing an UnsupportedOperationException if not.
     *
     * @param catalog The catalog instance
     * @return The SupportsNamespaces instance
     */
    public static SupportsNamespaces asSupportsNamespaces(Catalog catalog) {
        if (catalog instanceof SupportsNamespaces supportsNamespaces) {
            return supportsNamespaces;
        }
        throw new UnsupportedOperationException(
            "Catalog '" + (catalog != null ? catalog.name() : "null") + "' does not support namespace management (does not implement SupportsNamespaces)."
        );
    }

    /**
     * Resolves, validates, and initializes the Apache Iceberg Catalog instance.
     *
     * @param runContext The current Kestra RunContext
     * @return The initialized Iceberg Catalog
     */
    protected Catalog catalog(RunContext runContext) throws Exception {
        Map<String, String> rendered = runContext.render(catalogConfig).asMap(String.class, String.class);
        if (rendered == null || rendered.isEmpty()) {
            throw new IllegalArgumentException("'catalogConfig' must be provided with at least 'type' or 'catalog-impl'.");
        }

        String type = rendered.get("type");
        String catalogImpl = rendered.get("catalog-impl");

        if (type == null && catalogImpl == null) {
            throw new IllegalArgumentException(
                "'catalogConfig' must contain either 'type' (supported: " + SUPPORTED_TYPES + ") or 'catalog-impl'."
            );
        }

        if (type != null) {
            String normalizedType = type.trim().toLowerCase(Locale.ROOT);
            if (!SUPPORTED_TYPES.contains(normalizedType)) {
                throw new IllegalArgumentException(
                    "Unsupported catalog type '" + type + "'. Supported types are: " + SUPPORTED_TYPES + " (or supply 'catalog-impl')."
                );
            }
        }

        // CatalogUtil.buildIcebergCatalog() requires a Hadoop Configuration for Parquet/S3
        // FileIO initialization. We create a minimal Configuration and populate it with the
        // rendered catalog properties so that keys such as 's3.endpoint' or 'io-impl' are
        // picked up by the underlying FileIO. Note: all catalog properties — including any
        // sensitive keys that were already present in the rendered map — are forwarded here.
        // Callers are responsible for ensuring that only necessary properties are supplied.
        Configuration hadoopConf = new Configuration();
        for (Map.Entry<String, String> entry : rendered.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                hadoopConf.set(entry.getKey(), entry.getValue());
            }
        }

        Catalog catalog = CatalogUtil.buildIcebergCatalog("kestra", rendered, hadoopConf);
        runContext.logger().debug("Initialized Iceberg catalog with configuration: {}", redact(rendered));
        return catalog;
    }
}
