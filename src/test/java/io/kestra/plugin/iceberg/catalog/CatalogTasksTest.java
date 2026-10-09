package io.kestra.plugin.iceberg.catalog;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
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
        "catalog-impl", SharedInMemoryCatalog.class.getName(),
        "warehouse", "memory://test-warehouse"
    );

    @BeforeEach
    void setUp() {
        SharedInMemoryCatalog.reset();
    }

    @Test
    void shouldCreateListAndDropNamespaces() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create Namespace
        CreateNamespace createNs = CreateNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("analytics.marketing"))
            .properties(Property.of(Map.of("owner", "data-team", "env", "test")))
            .ifNotExists(Property.of(true))
            .build();

        CreateNamespace.Output createOutput = createNs.run(runContext);
        assertThat(createOutput.getNamespace(), is("analytics.marketing"));
        assertThat(createOutput.getCreated(), is(true));
        assertThat(createOutput.getProperties().get("owner"), is("data-team"));

        // 2. Create again with ifNotExists = true should not fail
        CreateNamespace.Output secondCreate = createNs.run(runContext);
        assertThat(secondCreate.getCreated(), is(false));

        // 3. Create again with ifNotExists = false should throw
        CreateNamespace strictCreate = CreateNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("analytics.marketing"))
            .ifNotExists(Property.of(false))
            .build();
        assertThrows(AlreadyExistsException.class, () -> strictCreate.run(runContext));

        // 4. List namespaces
        ListNamespaces listNs = ListNamespaces.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("analytics"))
            .build();
        ListNamespaces.Output listOutput = listNs.run(runContext);
        assertThat(listOutput.getNamespaces(), hasItem("analytics.marketing"));

        // 5. Drop namespace
        DropNamespace dropNs = DropNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("analytics.marketing"))
            .ifExists(Property.of(true))
            .build();
        DropNamespace.Output dropOutput = dropNs.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));

        // 6. Drop again with ifExists = true should return dropped = false
        DropNamespace.Output secondDrop = dropNs.run(runContext);
        assertThat(secondDrop.getDropped(), is(false));

        // 7. Drop with ifExists = false should throw
        DropNamespace strictDrop = DropNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("analytics.marketing"))
            .ifExists(Property.of(false))
            .build();
        assertThrows(NoSuchNamespaceException.class, () -> strictDrop.run(runContext));
    }

    @Test
    void shouldCreateAndDropTable() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Ensure namespace exists first
        CreateNamespace createNs = CreateNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("db"))
            .build();
        createNs.run(runContext);

        // 1. Create table
        CreateTable createTable = CreateTable.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("db"))
            .tableName(Property.of("events"))
            .columns(Property.of(List.of(
                CreateTable.Column.builder().name("id").type("long").required(true).build(),
                CreateTable.Column.builder().name("event_time").type("timestamptz").required(true).build(),
                CreateTable.Column.builder().name("payload").type("string").build()
            )))
            .partitionFields(Property.of(List.of("days(event_time)")))
            .tableProperties(Property.of(Map.of("write.format.default", "parquet")))
            .ifNotExists(Property.of(true))
            .build();

        CreateTable.Output createOutput = createTable.run(runContext);
        assertThat(createOutput.getTable(), is("db.events"));
        assertThat(createOutput.getCreated(), is(true));
        assertThat(createOutput.getLocation(), is(notNullValue()));

        // 2. Create again with ifNotExists = true
        CreateTable.Output secondCreate = createTable.run(runContext);
        assertThat(secondCreate.getCreated(), is(false));

        // 3. Drop table
        DropTable dropTable = DropTable.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("db"))
            .tableName(Property.of("events"))
            .purge(Property.of(false))
            .ifExists(Property.of(true))
            .build();

        DropTable.Output dropOutput = dropTable.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));

        // 4. Drop non-existent table with ifExists = false should throw
        DropTable strictDrop = DropTable.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("db"))
            .tableName(Property.of("events"))
            .ifExists(Property.of(false))
            .build();
        assertThrows(NoSuchTableException.class, () -> strictDrop.run(runContext));
    }

    @Test
    void shouldCreateTableWithRawJsonSchemaAndPurge() throws Exception {
        RunContext runContext = runContextFactory.of();

        CreateNamespace createNs = CreateNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("raw_db"))
            .build();
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

        CreateTable createTable = CreateTable.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("raw_db"))
            .tableName(Property.of("records"))
            .schemaJson(Property.of(jsonSchema))
            .build();

        CreateTable.Output createOutput = createTable.run(runContext);
        assertThat(createOutput.getTable(), is("raw_db.records"));
        assertThat(createOutput.getCreated(), is(true));

        // Purge drop
        DropTable dropTable = DropTable.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("raw_db"))
            .tableName(Property.of("records"))
            .purge(Property.of(true))
            .build();

        DropTable.Output dropOutput = dropTable.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));
    }

    @Test
    void shouldCascadeDropNamespaceWithTables() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create namespace
        CreateNamespace createNs = CreateNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("cascade_db"))
            .build();
        createNs.run(runContext);

        // 2. Create table inside namespace
        CreateTable createTable = CreateTable.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("cascade_db"))
            .tableName(Property.of("t1"))
            .columns(Property.of(List.of(CreateTable.Column.builder().name("id").type("int").build())))
            .build();
        createTable.run(runContext);

        // 3. Drop namespace with cascade = true
        DropNamespace dropNs = DropNamespace.builder()
            .catalogConfig(Property.of(CATALOG_CONFIG))
            .namespace(Property.of("cascade_db"))
            .cascade(Property.of(true))
            .build();

        DropNamespace.Output dropOutput = dropNs.run(runContext);
        assertThat(dropOutput.getDropped(), is(true));
    }
}
