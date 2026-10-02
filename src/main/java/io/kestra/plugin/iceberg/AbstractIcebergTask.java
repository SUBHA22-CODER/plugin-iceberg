package io.kestra.plugin.iceberg;

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
import org.slf4j.Logger;

import java.util.*;
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

    public static final Set<String> SUPPORTED_TYPES = Collections.unmodifiableSet(
        new LinkedHashSet<>(Arrays.asList("rest", "glue", "hive", "nessie", "hadoop"))
    );

    private static final Pattern SECRET_KEY_PATTERN = Pattern.compile(
        "(?i)(credential|token|secret|password|access[-_.]?key|session)"
    );

    @Schema(
        title = "Iceberg catalog configuration properties",
        description = "Key-value configuration map passed directly to the Iceberg catalog. " +
            "Requires 'type' (e.g. 'rest', 'glue', 'hive', 'nessie') or 'catalog-impl' (fully-qualified class name)."
    )
    @NotNull
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

        // Initialize Hadoop Configuration for Iceberg catalog/FileIO resolution
        Configuration hadoopConf = new Configuration();

        // Pass any non-secret hadoop/s3 configurations into Hadoop Configuration
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
