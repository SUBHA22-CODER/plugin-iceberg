package io.kestra.plugin.iceberg.catalog;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IcebergSchemaUtilsTest {

    @Test
    void shouldParsePrimitiveTypes() {
        assertThat(IcebergSchemaUtils.parseType("boolean"), is(instanceOf(Types.BooleanType.class)));
        assertThat(IcebergSchemaUtils.parseType("int"), is(instanceOf(Types.IntegerType.class)));
        assertThat(IcebergSchemaUtils.parseType("integer"), is(instanceOf(Types.IntegerType.class)));
        assertThat(IcebergSchemaUtils.parseType("long"), is(instanceOf(Types.LongType.class)));
        assertThat(IcebergSchemaUtils.parseType("bigint"), is(instanceOf(Types.LongType.class)));
        assertThat(IcebergSchemaUtils.parseType("float"), is(instanceOf(Types.FloatType.class)));
        assertThat(IcebergSchemaUtils.parseType("double"), is(instanceOf(Types.DoubleType.class)));
        assertThat(IcebergSchemaUtils.parseType("date"), is(instanceOf(Types.DateType.class)));
        assertThat(IcebergSchemaUtils.parseType("time"), is(instanceOf(Types.TimeType.class)));
        assertThat(IcebergSchemaUtils.parseType("string"), is(instanceOf(Types.StringType.class)));
        assertThat(IcebergSchemaUtils.parseType("uuid"), is(instanceOf(Types.UUIDType.class)));
        assertThat(IcebergSchemaUtils.parseType("binary"), is(instanceOf(Types.BinaryType.class)));
    }

    @Test
    void shouldParseTimestampWithAndWithoutZone() {
        Types.TimestampType withoutZone = (Types.TimestampType) IcebergSchemaUtils.parseType("timestamp");
        assertThat(withoutZone.shouldAdjustToUTC(), is(false));

        Types.TimestampType withZone = (Types.TimestampType) IcebergSchemaUtils.parseType("timestamptz");
        assertThat(withZone.shouldAdjustToUTC(), is(true));
    }

    @Test
    void shouldParseParameterizedTypes() {
        Types.DecimalType decimal = (Types.DecimalType) IcebergSchemaUtils.parseType("decimal(18, 4)");
        assertThat(decimal.precision(), is(18));
        assertThat(decimal.scale(), is(4));

        Types.FixedType fixed = (Types.FixedType) IcebergSchemaUtils.parseType("fixed(16)");
        assertThat(fixed.length(), is(16));
    }

    @Test
    void shouldThrowOnInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> IcebergSchemaUtils.parseType("unknown_type"));
        assertThrows(IllegalArgumentException.class, () -> IcebergSchemaUtils.parseType(""));
        assertThrows(IllegalArgumentException.class, () -> IcebergSchemaUtils.parseType(null));
    }

    @Test
    void shouldBuildSchemaFromColumns() {
        List<CreateTable.Column> columns = List.of(
            CreateTable.Column.builder().name("id").type("long").required(true).doc("primary key").build(),
            CreateTable.Column.builder().name("username").type("string").required(false).build(),
            CreateTable.Column.builder().name("balance").type("decimal(10,2)").required(true).build()
        );

        Schema schema = IcebergSchemaUtils.buildSchema(null, columns);

        assertThat(schema.columns(), hasSize(3));
        assertThat(schema.findField("id").isRequired(), is(true));
        assertThat(schema.findField("id").doc(), is("primary key"));
        assertThat(schema.findField("username").isOptional(), is(true));
        assertThat(schema.findField("balance").type(), is(instanceOf(Types.DecimalType.class)));
    }

    @Test
    void shouldBuildSchemaFromJson() {
        String json = """
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                {"id": 1, "name": "event_id", "required": true, "type": "long"},
                {"id": 2, "name": "payload", "required": false, "type": "string"}
              ]
            }
            """;

        Schema schema = IcebergSchemaUtils.buildSchema(json, null);

        assertThat(schema.columns(), hasSize(2));
        assertThat(schema.findField("event_id").isRequired(), is(true));
        assertThat(schema.findField("payload").type(), is(instanceOf(Types.StringType.class)));
    }

    @Test
    void shouldBuildPartitionSpec() {
        List<CreateTable.Column> columns = List.of(
            CreateTable.Column.builder().name("id").type("long").build(),
            CreateTable.Column.builder().name("event_time").type("timestamptz").build(),
            CreateTable.Column.builder().name("region").type("string").build(),
            CreateTable.Column.builder().name("category").type("string").build()
        );
        Schema schema = IcebergSchemaUtils.buildSchema(null, columns);

        List<String> partitionFields = List.of(
            "days(event_time)",
            "identity(region)",
            "bucket(16, id)",
            "truncate(4, category)"
        );

        PartitionSpec spec = IcebergSchemaUtils.buildPartitionSpec(schema, partitionFields);

        assertThat(spec.isPartitioned(), is(true));
        assertThat(spec.fields(), hasSize(4));
    }

    @Test
    void shouldBuildUnpartitionedSpecWhenEmpty() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        PartitionSpec spec = IcebergSchemaUtils.buildPartitionSpec(schema, null);
        assertThat(spec.isUnpartitioned(), is(true));
    }

    @Test
    void shouldThrowWhenBothSchemaJsonAndColumnsProvided() {
        String json = "{\"type\": \"struct\", \"fields\": [{\"id\": 1, \"name\": \"id\", \"type\": \"long\"}]}";
        List<CreateTable.Column> columns = List.of(
            CreateTable.Column.builder().name("id").type("long").build()
        );

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> IcebergSchemaUtils.buildSchema(json, columns)
        );
        assertThat(thrown.getMessage(), containsString("Cannot provide both 'schemaJson' and 'columns'"));
    }

    @Test
    void shouldThrowWhenNeitherSchemaJsonNorColumnsProvided() {
        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> IcebergSchemaUtils.buildSchema(null, null)
        );
        assertThat(thrown.getMessage(), containsString("Either 'schemaJson' or at least one 'columns'"));
    }

    @Test
    void shouldThrowWhenPartitionFieldDoesNotExistInSchema() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        List<String> partitionFields = List.of("missing_field");

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> IcebergSchemaUtils.buildPartitionSpec(schema, partitionFields)
        );
        assertThat(thrown.getMessage(), containsString("Partition field 'missing_field' does not exist"));
    }
}
