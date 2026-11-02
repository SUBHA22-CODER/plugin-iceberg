package io.kestra.plugin.iceberg.catalog;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.iceberg.AbstractIcebergTask;
import io.kestra.plugin.iceberg.TestCatalogs;
import jakarta.inject.Inject;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CatalogTasksTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static final Map<String, String> CATALOG_CONFIG = Map.of(
        "type", "rest",
        "uri", "http://unused.invalid"
    );

    private static final SharedInMemoryCatalog CATALOG = new SharedInMemoryCatalog();

    static {
        CATALOG.initialize("test", Map.of("warehouse", "memory://test-warehouse"));
    }

    private static <T extends AbstractIcebergTask> T inMemory(T task) {
        return TestCatalogs.withCatalog(task, CATALOG);
    }

    @BeforeEach
    void setUp() {
        SharedInMemoryCatalog.reset();
    }

    @Test
    void shouldCreateListAndDropNamespaces() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create Namespace
        CreateNamespace createNs = inMemory(CreateNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("analytics.marketing"))
            .properties(Property.ofValue(Map.of("owner", "data-team", "env", "test")))
            .ifNotExists(Property.ofValue(true))
            .build());

        CreateNamespace.Output createOutput = createNs.run(runContext);
        assertThat(createOutput.getNamespace(), is("analytics.marketing"));
        assertThat(createOutput.getCreated(), is(true));
        assertThat(createOutput.getProperties().get("owner"), is("data-team"));

        // 2. Create again with ifNotExists = true should not fail
        CreateNamespace.Output secondCreate = createNs.run(runContext);
        assertThat(secondCreate.getCreated(), is(false));

        // 3. Create again with ifNotExists = false should throw
        CreateNamespace strictCreate = inMemory(CreateNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("analytics.marketing"))
            .ifNotExists(Property.ofValue(false))
            .build());
        assertThrows(AlreadyExistsException.class, () -> strictCreate.run(runContext));

        // 4. List namespaces
        ListNamespaces listNs = inMemory(ListNamespaces.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("analytics"))
            .build());
        ListNamespaces.Output listOutput = listNs.run(runContext);
        assertThat(listOutput.getNamespaces(), hasItem("analytics.marketing"));

        // 5. Drop namespace
        DropNamespace dropNs = inMemory(DropNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("analytics.marketing"))
            .ifExists(Property.ofValue(true))
            .build());
        DropNamespace.Output dropOutput = dropNs.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));

        // 6. Drop again with ifExists = true should return dropped = false
        DropNamespace.Output secondDrop = dropNs.run(runContext);
        assertThat(secondDrop.getDropped(), is(false));

        // 7. Drop with ifExists = false should throw
        DropNamespace strictDrop = inMemory(DropNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("analytics.marketing"))
            .ifExists(Property.ofValue(false))
            .build());
        assertThrows(NoSuchNamespaceException.class, () -> strictDrop.run(runContext));
    }

    @Test
    void shouldCreateAndDropTable() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Ensure namespace exists first
        CreateNamespace createNs = inMemory(CreateNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .build());
        createNs.run(runContext);

        // 1. Create table
        CreateTable createTable = inMemory(CreateTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .tableName(Property.ofValue("events"))
            .columns(Property.ofValue(List.of(
                CreateTable.Column.builder().name("id").type("long").required(true).build(),
                CreateTable.Column.builder().name("event_time").type("timestamptz").required(true).build(),
                CreateTable.Column.builder().name("payload").type("string").build()
            )))
            .partitionFields(Property.ofValue(List.of("days(event_time)")))
            .tableProperties(Property.ofValue(Map.of("write.format.default", "parquet")))
            .ifNotExists(Property.ofValue(true))
            .build());

        CreateTable.Output createOutput = createTable.run(runContext);
        assertThat(createOutput.getTable(), is("db.events"));
        assertThat(createOutput.getCreated(), is(true));
        assertThat(createOutput.getLocation(), is(notNullValue()));

        // 2. Create again with ifNotExists = true
        CreateTable.Output secondCreate = createTable.run(runContext);
        assertThat(secondCreate.getCreated(), is(false));

        // 3. Create again with omitted ifNotExists (default = true)
        CreateTable defaultIfNotExistsTable = inMemory(CreateTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .tableName(Property.ofValue("events"))
            .columns(Property.ofValue(List.of(
                CreateTable.Column.builder().name("id").type("long").required(true).build()
            )))
            .build());
        CreateTable.Output defaultCreateOutput = defaultIfNotExistsTable.run(runContext);
        assertThat(defaultCreateOutput.getCreated(), is(false));

        // 4. Create again with ifNotExists = false should throw
        CreateTable strictCreateTable = inMemory(CreateTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .tableName(Property.ofValue("events"))
            .columns(Property.ofValue(List.of(
                CreateTable.Column.builder().name("id").type("long").required(true).build()
            )))
            .ifNotExists(Property.ofValue(false))
            .build());
        assertThrows(AlreadyExistsException.class, () -> strictCreateTable.run(runContext));

        // 5. Drop table
        DropTable dropTable = inMemory(DropTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .tableName(Property.ofValue("events"))
            .purge(Property.ofValue(false))
            .ifExists(Property.ofValue(true))
            .build());

        DropTable.Output dropOutput = dropTable.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));

        // 4. Drop non-existent table with ifExists = false should throw
        DropTable strictDrop = inMemory(DropTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .tableName(Property.ofValue("events"))
            .ifExists(Property.ofValue(false))
            .build());
        assertThrows(NoSuchTableException.class, () -> strictDrop.run(runContext));
    }

    @Test
    void shouldCreateTableWithRawJsonSchemaAndPurge() throws Exception {
        RunContext runContext = runContextFactory.of();

        CreateNamespace createNs = inMemory(CreateNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("raw_db"))
            .build());
        createNs.run(runContext);

        String jsonSchema = """
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                {"id": 1, "name": "uid", "required": true, "type": "long"},
                {"id": 2, "name": "data", "required": false, "type": "string"}
              ]
            }
            """;

        CreateTable createTable = inMemory(CreateTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("raw_db"))
            .tableName(Property.ofValue("records"))
            .schemaJson(Property.ofValue(jsonSchema))
            .build());

        CreateTable.Output createOutput = createTable.run(runContext);
        assertThat(createOutput.getTable(), is("raw_db.records"));
        assertThat(createOutput.getCreated(), is(true));

        // Purge drop
        DropTable dropTable = inMemory(DropTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("raw_db"))
            .tableName(Property.ofValue("records"))
            .purge(Property.ofValue(true))
            .build());

        DropTable.Output dropOutput = dropTable.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));
    }

    @Test
    void shouldCascadeDropNamespaceWithTables() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create namespace
        CreateNamespace createNs = inMemory(CreateNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("cascade_db"))
            .build());
        createNs.run(runContext);

        // 2. Create table inside namespace
        CreateTable createTable = inMemory(CreateTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("cascade_db"))
            .tableName(Property.ofValue("t1"))
            .columns(Property.ofValue(List.of(CreateTable.Column.builder().name("id").type("int").build())))
            .build());
        createTable.run(runContext);

        // 3. Drop namespace with cascade = true
        DropNamespace dropNs = inMemory(DropNamespace.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("cascade_db"))
            .cascade(Property.ofValue(true))
            .build());

        DropNamespace.Output dropOutput = dropNs.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));
    }

    @Test
    void shouldThrowWhenCreateTableHasBothSchemaJsonAndColumns() {
        RunContext runContext = runContextFactory.of();

        CreateTable createTable = inMemory(CreateTable.builder()
            .catalogConfig(Property.ofValue(CATALOG_CONFIG))
            .namespace(Property.ofValue("db"))
            .tableName(Property.ofValue("invalid_table"))
            .columns(Property.ofValue(List.of(
                CreateTable.Column.builder().name("id").type("long").build()
            )))
            .schemaJson(Property.ofValue("{\"type\": \"struct\", \"fields\": [{\"id\": 1, \"name\": \"id\", \"type\": \"long\"}]}"))
            .build());

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> createTable.run(runContext));
        assertThat(thrown.getMessage(), containsString("Cannot provide both 'schemaJson' and 'columns'. Please specify only one."));
    }

    @Test
    void shouldVerifyBuilderDefaultsAndExplicitConfigurations() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Verify default values instantiated via Lombok builder
        CreateNamespace defaultCreateNs = inMemory(CreateNamespace.builder().build());
        assertThat(runContext.render(defaultCreateNs.getIfNotExists()).as(Boolean.class).orElse(null), is(true));

        CreateTable defaultCreateTable = inMemory(CreateTable.builder().build());
        assertThat(runContext.render(defaultCreateTable.getIfNotExists()).as(Boolean.class).orElse(null), is(true));

        DropNamespace defaultDropNs = inMemory(DropNamespace.builder().build());
        assertThat(runContext.render(defaultDropNs.getIfExists()).as(Boolean.class).orElse(null), is(true));
        assertThat(runContext.render(defaultDropNs.getCascade()).as(Boolean.class).orElse(null), is(false));

        DropTable defaultDropTable = inMemory(DropTable.builder().build());
        assertThat(runContext.render(defaultDropTable.getIfExists()).as(Boolean.class).orElse(null), is(true));
        assertThat(runContext.render(defaultDropTable.getPurge()).as(Boolean.class).orElse(null), is(false));

        // 2. Verify explicitly configured values
        CreateNamespace explicitCreateNs = inMemory(CreateNamespace.builder()
            .ifNotExists(Property.ofValue(false))
            .build());
        assertThat(runContext.render(explicitCreateNs.getIfNotExists()).as(Boolean.class).orElse(null), is(false));

        CreateTable explicitCreateTable = inMemory(CreateTable.builder()
            .ifNotExists(Property.ofValue(false))
            .build());
        assertThat(runContext.render(explicitCreateTable.getIfNotExists()).as(Boolean.class).orElse(null), is(false));

        DropNamespace explicitDropNs = inMemory(DropNamespace.builder()
            .ifExists(Property.ofValue(false))
            .cascade(Property.ofValue(true))
            .build());
        assertThat(runContext.render(explicitDropNs.getIfExists()).as(Boolean.class).orElse(null), is(false));
        assertThat(runContext.render(explicitDropNs.getCascade()).as(Boolean.class).orElse(null), is(true));

        DropTable explicitDropTable = inMemory(DropTable.builder()
            .ifExists(Property.ofValue(false))
            .purge(Property.ofValue(true))
            .build());
        assertThat(runContext.render(explicitDropTable.getIfExists()).as(Boolean.class).orElse(null), is(false));
        assertThat(runContext.render(explicitDropTable.getPurge()).as(Boolean.class).orElse(null), is(true));
    }
}
