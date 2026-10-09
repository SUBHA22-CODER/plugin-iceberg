package io.kestra.plugin.iceberg;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and lightweight integration tests for {@link AbstractIcebergTask} and
 * {@link AbstractIcebergTableTask}.
 *
 * <p>Pure-logic tests (redact, tableIdentifier, closeCatalog) run without any
 * framework; the Pebble-rendering test uses a real Kestra {@link RunContext} via
 * {@link RunContextFactory} so that expression rendering is exercised end-to-end.
 */
@KestraTest
class AbstractIcebergTaskTest {

    // -------------------------------------------------------------------------
    // Minimal concrete task helpers
    // -------------------------------------------------------------------------

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class DummyIcebergTask extends AbstractIcebergTask {
        public Catalog testCatalog(RunContext runContext) throws Exception {
            return this.catalog(runContext);
        }
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class DummyIcebergTableTask extends AbstractIcebergTableTask {
        public TableIdentifier testTableIdentifier(RunContext runContext) throws Exception {
            return this.tableIdentifier(runContext);
        }
    }

    // -------------------------------------------------------------------------
    // RunContextFactory injection (used by the Pebble-rendering test only)
    // -------------------------------------------------------------------------

    @Inject
    private RunContextFactory runContextFactory;

    // -------------------------------------------------------------------------
    // Pure unit tests – no Micronaut context required
    // -------------------------------------------------------------------------

    @Test
    void shouldRedactSensitiveKeys() {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("type", "rest");
        raw.put("uri", "https://iceberg.example.com");
        raw.put("credential", "secret-oauth-credential");
        raw.put("token", "bearer-token-123");
        raw.put("s3.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        raw.put("s3.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        raw.put("password", "supersecret");
        raw.put("session.token", "session-xyz");

        Map<String, String> redacted = AbstractIcebergTask.redact(raw);

        assertThat(redacted.get("type"), is("rest"));
        assertThat(redacted.get("uri"), is("https://iceberg.example.com"));
        assertThat(redacted.get("credential"), is("******"));
        assertThat(redacted.get("token"), is("******"));
        assertThat(redacted.get("s3.access-key-id"), is("******"));
        assertThat(redacted.get("s3.secret-access-key"), is("******"));
        assertThat(redacted.get("password"), is("******"));
        assertThat(redacted.get("session.token"), is("******"));
    }

    @Test
    void shouldHandleEmptyOrNullRedaction() {
        assertThat(AbstractIcebergTask.redact(null), is(Collections.emptyMap()));
        assertThat(AbstractIcebergTask.redact(Collections.emptyMap()), is(Collections.emptyMap()));
    }

    @Test
    void shouldExcludeCatalogConfigFromToString() {
        DummyIcebergTask task = DummyIcebergTask.builder()
            .id("test-task")
            .catalogConfig(Property.of(Map.of(
                "type", "rest",
                "uri", "https://iceberg.example.com",
                "credential", "super-secret-token"
            )))
            .build();

        String str = task.toString();
        assertThat(str, not(containsString("super-secret-token")));
        assertThat(str, not(containsString("catalogConfig")));
    }

    @Test
    void shouldFailWhenCatalogTypeIsUnsupported() {
        // Only 'rest' is supported in Phase 1
        RunContext runContext = runContextFactory.of();

        DummyIcebergTask task = DummyIcebergTask.builder()
            .catalogConfig(new Property<>(Map.of("type", "unsupported-catalog")))
            .build();

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> task.testCatalog(runContext)
        );

        assertThat(thrown.getMessage(), containsString("Unsupported catalog type 'unsupported-catalog'"));
        assertThat(thrown.getMessage(), containsString("rest"));
    }

    @Test
    void shouldFailWhenCatalogConfigMissingTypeAndImpl() {
        RunContext runContext = runContextFactory.of();

        DummyIcebergTask task = DummyIcebergTask.builder()
            .catalogConfig(new Property<>(Map.of("warehouse", "s3://my-bucket/warehouse")))
            .build();

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> task.testCatalog(runContext)
        );

        assertThat(thrown.getMessage(), containsString("must contain either 'type'"));
    }

    @Test
    void shouldParseTableIdentifierSingleLevel() throws Exception {
        RunContext runContext = runContextFactory.of();

        DummyIcebergTableTask task = DummyIcebergTableTask.builder()
            .namespace(Property.of("analytics"))
            .tableName(Property.of("events"))
            .build();

        TableIdentifier identifier = task.testTableIdentifier(runContext);

        assertThat(identifier.namespace().levels(), arrayContaining("analytics"));
        assertThat(identifier.name(), is("events"));
    }

    @Test
    void shouldParseTableIdentifierMultiLevel() throws Exception {
        RunContext runContext = runContextFactory.of();

        DummyIcebergTableTask task = DummyIcebergTableTask.builder()
            .namespace(Property.of("analytics.raw.v1"))
            .tableName(Property.of("pageviews"))
            .build();

        TableIdentifier identifier = task.testTableIdentifier(runContext);

        assertThat(identifier.namespace().levels(), arrayContaining("analytics", "raw", "v1"));
        assertThat(identifier.name(), is("pageviews"));
    }

    @Test
    void shouldSafelyCloseAutoCloseableCatalog() {
        AutoCloseableCatalog mockCatalog = new AutoCloseableCatalog();
        assertFalse(mockCatalog.closed);

        AbstractIcebergTask.closeCatalog(mockCatalog, null);
        assertTrue(mockCatalog.closed);
    }

    // -------------------------------------------------------------------------
    // Pebble expression rendering — uses real RunContext
    // -------------------------------------------------------------------------

    @Test
    void shouldRenderCatalogConfigExpressionsWithRealRunContext() throws Exception {
        // Use new Property<>(map) so Pebble expressions are rendered by the real engine
        RunContext runContext = runContextFactory.of(
            Map.of("vars", Map.of(
                "rest_uri", "http://localhost:8181",
                "secret_token", "my-secret-token"
            ))
        );

        DummyIcebergTask task = DummyIcebergTask.builder()
            .catalogConfig(new Property<>(Map.of(
                "type", "rest",
                "uri", "{{ vars.rest_uri }}",
                "token", "{{ vars.secret_token }}"
            )))
            .build();

        Map<String, String> rendered = runContext.render(task.getCatalogConfig())
            .asMap(String.class, String.class);

        assertThat(rendered.get("type"), is("rest"));
        assertThat(rendered.get("uri"), is("http://localhost:8181"));
        assertThat(rendered.get("token"), is("my-secret-token"));

        // Verify that the rendered token is properly redacted for logging
        Map<String, String> safe = AbstractIcebergTask.redact(rendered);
        assertThat(safe.get("token"), is("******"));
    }

    // -------------------------------------------------------------------------
    // Minimal Catalog stub for the closeCatalog test
    // -------------------------------------------------------------------------

    static class AutoCloseableCatalog implements Catalog, AutoCloseable {
        boolean closed = false;

        @Override
        public void close() {
            this.closed = true;
        }

        @Override
        public String name() {
            return "test";
        }

        @Override
        public List<TableIdentifier> listTables(org.apache.iceberg.catalog.Namespace namespace) {
            return Collections.emptyList();
        }

        @Override
        public boolean dropTable(TableIdentifier identifier, boolean purge) {
            return false;
        }

        @Override
        public void renameTable(TableIdentifier from, TableIdentifier to) {}

        @Override
        public org.apache.iceberg.Table loadTable(TableIdentifier identifier) {
            return null;
        }

        @Override
        public org.apache.iceberg.Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
            return null;
        }

        @Override
        public org.apache.iceberg.catalog.Catalog.TableBuilder buildTable(
            TableIdentifier identifier, org.apache.iceberg.Schema schema) {
            return null;
        }

        @Override
        public void initialize(String name, Map<String, String> properties) {}
    }
}
