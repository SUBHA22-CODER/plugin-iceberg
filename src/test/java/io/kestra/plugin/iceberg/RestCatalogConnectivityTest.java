package io.kestra.plugin.iceberg;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class RestCatalogConnectivityTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldAttemptRestCatalogConnectionAndNotLeakTokenOnFailure() {
        AbstractIcebergTaskTest.DummyIcebergTask task = AbstractIcebergTaskTest.DummyIcebergTask.builder()
            .catalogConfig(Property.of(Map.of(
                "type", "rest",
                "uri", "http://127.0.0.1:58181/unreachable",
                "token", "super-secret-token-12345"
            )))
            .build();

        RunContext runContext = runContextFactory.of();

        Exception thrown = assertThrows(
            Exception.class,
            () -> task.testCatalog(runContext)
        );

        // Verify the failure occurred during REST connection and the secret was not exposed in the exception message
        assertThat(thrown.getMessage(), not(containsString("super-secret-token-12345")));
    }

    @Test
    @EnabledIf(
        value = "isIntegrationEnabled",
        disabledReason = "Iceberg REST catalog integration test disabled; set ICEBERG_REST_INTEGRATION=true or -Diceberg.rest.integration=true to enable."
    )
    void shouldConnectToRestCatalogAndListNamespaces() throws Exception {
        String restUri = System.getProperty(
            "iceberg.rest.uri",
            System.getenv().getOrDefault("ICEBERG_REST_URI", "http://localhost:8181")
        );

        AbstractIcebergTaskTest.DummyIcebergTask task = AbstractIcebergTaskTest.DummyIcebergTask.builder()
            .catalogConfig(Property.of(Map.of(
                "type", "rest",
                "uri", restUri
            )))
            .build();

        RunContext runContext = runContextFactory.of();

        Catalog catalog = task.testCatalog(runContext);
        try {
            assertThat(catalog, is(notNullValue()));
            assertThat(catalog, is(instanceOf(RESTCatalog.class)));

            RESTCatalog restCatalog = (RESTCatalog) catalog;
            List<Namespace> namespaces = restCatalog.listNamespaces();
            assertThat(namespaces, is(notNullValue()));
        } finally {
            AbstractIcebergTask.closeCatalog(catalog, runContext.logger());
        }
    }

    static boolean isIntegrationEnabled() {
        return Boolean.parseBoolean(System.getProperty("iceberg.rest.integration"))
            || Boolean.parseBoolean(System.getenv("ICEBERG_REST_INTEGRATION"));
    }
}
