package io.kestra.plugin.iceberg.catalog;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public final class IcebergSchemaUtils {

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("^decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)$");
    private static final Pattern FIXED_PATTERN = Pattern.compile("^fixed\\s*\\(\\s*(\\d+)\\s*\\)$");
    private static final Pattern BUCKET_PATTERN = Pattern.compile("(?i)^bucket\\s*\\(\\s*(\\d+)\\s*,\\s*([a-zA-Z0-9_.-]+)\\s*\\)$");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("(?i)^truncate\\s*\\(\\s*(\\d+)\\s*,\\s*([a-zA-Z0-9_.-]+)\\s*\\)$");
    private static final Pattern TRANSFORM_PATTERN = Pattern.compile("(?i)^(identity|year|years|month|months|day|days|date|hour|hours)\\s*\\(\\s*([a-zA-Z0-9_.-]+)\\s*\\)$");

    private IcebergSchemaUtils() {}

    public static Schema buildSchema(String schemaJson, List<CreateTable.Column> columns) {
        var hasJson = schemaJson != null && !schemaJson.trim().isEmpty();
        var hasColumns = columns != null && !columns.isEmpty();

        if (hasJson && hasColumns) {
            throw new IllegalArgumentException("Cannot provide both 'schemaJson' and 'columns'. Please specify only one.");
        }
        if (!hasJson && !hasColumns) {
            throw new IllegalArgumentException("Either 'schemaJson' or at least one 'columns' definition must be provided.");
        }

        if (hasJson) {
            return SchemaParser.fromJson(schemaJson.trim());
        }

        var fields = new ArrayList<Types.NestedField>(columns.size());
        var fieldId = 1;
        for (var col : columns) {
            if (col.getName() == null || col.getName().trim().isEmpty()) {
                throw new IllegalArgumentException("Column name must not be null or empty.");
            }
            var type = parseType(col.getType());
            var required = Boolean.TRUE.equals(col.getRequired());
            if (required) {
                fields.add(Types.NestedField.required(fieldId++, col.getName().trim(), type, col.getDoc()));
            } else {
                fields.add(Types.NestedField.optional(fieldId++, col.getName().trim(), type, col.getDoc()));
            }
        }

        return new Schema(fields);
    }

    public static Type parseType(String typeStr) {
        if (typeStr == null || typeStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Column type must not be null or empty.");
        }
        var normalized = typeStr.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "boolean":
            case "bool":
                return Types.BooleanType.get();
            case "int":
            case "integer":
                return Types.IntegerType.get();
            case "long":
            case "bigint":
                return Types.LongType.get();
            case "float":
                return Types.FloatType.get();
            case "double":
                return Types.DoubleType.get();
            case "date":
                return Types.DateType.get();
            case "time":
                return Types.TimeType.get();
            case "timestamp":
            case "timestamp_ntz":
                return Types.TimestampType.withoutZone();
            case "timestamptz":
            case "timestamp_tz":
                return Types.TimestampType.withZone();
            case "string":
            case "text":
            case "varchar":
                return Types.StringType.get();
            case "uuid":
                return Types.UUIDType.get();
            case "binary":
                return Types.BinaryType.get();
            default:
                var decMatcher = DECIMAL_PATTERN.matcher(normalized);
                if (decMatcher.matches()) {
                    var precision = Integer.parseInt(decMatcher.group(1));
                    var scale = Integer.parseInt(decMatcher.group(2));
                    return Types.DecimalType.of(precision, scale);
                }
                var fixMatcher = FIXED_PATTERN.matcher(normalized);
                if (fixMatcher.matches()) {
                    var length = Integer.parseInt(fixMatcher.group(1));
                    return Types.FixedType.ofLength(length);
                }
                throw new IllegalArgumentException("Unsupported Iceberg data type: '" + typeStr + "'");
        }
    }

    public static PartitionSpec buildPartitionSpec(Schema schema, List<String> partitionFields) {
        if (partitionFields == null || partitionFields.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }

        var builder = PartitionSpec.builderFor(schema);
        for (var fieldExpr : partitionFields) {
            if (fieldExpr == null || fieldExpr.trim().isEmpty()) {
                continue;
            }
            var expr = fieldExpr.trim();
            var bucketMatcher = BUCKET_PATTERN.matcher(expr);
            var truncateMatcher = TRUNCATE_PATTERN.matcher(expr);
            var transformMatcher = TRANSFORM_PATTERN.matcher(expr);

            String col;
            if (bucketMatcher.matches()) {
                var numBuckets = Integer.parseInt(bucketMatcher.group(1));
                col = bucketMatcher.group(2);
                validateFieldExists(schema, col);
                builder.bucket(col, numBuckets);
            } else if (truncateMatcher.matches()) {
                var width = Integer.parseInt(truncateMatcher.group(1));
                col = truncateMatcher.group(2);
                validateFieldExists(schema, col);
                builder.truncate(col, width);
            } else if (transformMatcher.matches()) {
                var transform = transformMatcher.group(1).toLowerCase(Locale.ROOT);
                col = transformMatcher.group(2);
                validateFieldExists(schema, col);
                switch (transform) {
                    case "year":
                    case "years":
                        builder.year(col);
                        break;
                    case "month":
                    case "months":
                        builder.month(col);
                        break;
                    case "day":
                    case "days":
                    case "date":
                        builder.day(col);
                        break;
                    case "hour":
                    case "hours":
                        builder.hour(col);
                        break;
                    case "identity":
                    default:
                        builder.identity(col);
                        break;
                }
            } else {
                col = expr;
                validateFieldExists(schema, col);
                builder.identity(col);
            }
        }
        return builder.build();
    }

    private static void validateFieldExists(Schema schema, String fieldName) {
        if (schema.findField(fieldName) == null) {
            throw new IllegalArgumentException("Partition field '" + fieldName + "' does not exist in table schema.");
        }
    }
}
