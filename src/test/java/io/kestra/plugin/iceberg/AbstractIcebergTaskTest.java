package io.kestra.plugin.iceberg;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

public class AbstractIcebergTaskTest {

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

    @SuppressWarnings("unchecked")
    static <T> RunContextProperty<T> mockProperty(Object value) {
        RunContextProperty<T> rcp = Mockito.mock(RunContextProperty.class);
        try {
            Mockito.when(rcp.as(Mockito.any())).thenReturn(Optional.ofNullable((T) value));
            if (value instanceof Map mapVal) {
                Mockito.when(rcp.asMap(Mockito.any(), Mockito.any())).thenReturn((T) mapVal);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rcp;
    }

    static RunContext createMockRunContext(Map<String, Object> variables) {
        RunContext runContext = Mockito.mock(RunContext.class);
        Mockito.when(runContext.logger()).thenReturn(LoggerFactory.getLogger(AbstractIcebergTaskTest.class));

        Mockito.when(runContext.render(Mockito.any(Property.class))).thenAnswer(invocation -> {
            Property<?> prop = invocation.getArgument(0);
            Object value = extractPropertyValue(prop);

            if (value instanceof Map<?, ?> map) {
                Map<String, String> resolved = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String k = entry.getKey().toString();
                    String v = entry.getValue() != null ? entry.getValue().toString() : null;
                    if (v != null && variables != null) {
                        for (Map.Entry<String, Object> varEntry : variables.entrySet()) {
                            if (varEntry.getValue() instanceof Map<?, ?> nested) {
                                for (Map.Entry<?, ?> nestedEntry : nested.entrySet()) {
                                    v = v.replace("{{ " + varEntry.getKey() + "." + nestedEntry.getKey() + " }}", nestedEntry.getValue().toString());
                                }
                            } else if (varEntry.getValue() != null) {
                                v = v.replace("{{ " + varEntry.getKey() + " }}", varEntry.getValue().toString());
                            }
                        }
                    }
                    resolved.put(k, v);
                }
                return mockProperty(resolved);
            } else if (value instanceof String str && variables != null) {
                for (Map.Entry<String, Object> varEntry : variables.entrySet()) {
                    if (varEntry.getValue() != null) {
                        str = str.replace("{{ " + varEntry.getKey() + " }}", varEntry.getValue().toString());
                    }
                }
                return mockProperty(str);
            }

            return mockProperty(value);
        });

        return runContext;
    }

    private static Object extractPropertyValue(Property<?> prop) {
        if (prop == null) return null;
        try {
            Field field = Property.class.getDeclaredField("value");
            field.setAccessible(true);
            return field.get(prop);
        } catch (Exception e) {
            return null;
        }
    }

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
    void shouldFailWhenCatalogConfigMissingTypeAndImpl() {
        DummyIcebergTask task = DummyIcebergTask.builder()
            .catalogConfig(Property.of(Map.of("warehouse", "s3://my-bucket/warehouse")))
            .build();

        RunContext runContext = createMockRunContext(null);

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> task.testCatalog(runContext)
        );

        assertThat(thrown.getMessage(), containsString("must contain either 'type'"));
    }

    @Test
    void shouldFailWhenCatalogTypeIsUnsupported() {
        DummyIcebergTask task = DummyIcebergTask.builder()
            .catalogConfig(Property.of(Map.of("type", "unsupported-catalog")))
            .build();

        RunContext runContext = createMockRunContext(null);

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> task.testCatalog(runContext)
        );

        assertThat(thrown.getMessage(), containsString("Unsupported catalog type 'unsupported-catalog'"));
        assertThat(thrown.getMessage(), containsString("rest"));
    }

    @Test
    void shouldRenderCatalogConfigExpressions() throws Exception {
        DummyIcebergTask task = DummyIcebergTask.builder()
            .catalogConfig(Property.of(Map.of(
                "type", "rest",
                "uri", "{{ vars.rest_uri }}",
                "token", "{{ vars.secret_token }}"
            )))
            .build();

        RunContext runContext = createMockRunContext(Map.of(
            "vars", Map.of(
                "rest_uri", "http://localhost:8181",
                "secret_token", "my-secret-token"
            )
        ));

        Map<String, String> rendered = runContext.render(task.getCatalogConfig()).asMap(String.class, String.class);
        assertThat(rendered.get("type"), is("rest"));
        assertThat(rendered.get("uri"), is("http://localhost:8181"));
        assertThat(rendered.get("token"), is("my-secret-token"));

        Map<String, String> safe = AbstractIcebergTask.redact(rendered);
        assertThat(safe.get("token"), is("******"));
    }

    @Test
    void shouldParseTableIdentifierSingleLevel() throws Exception {
        DummyIcebergTableTask task = DummyIcebergTableTask.builder()
            .namespace(Property.of("analytics"))
            .tableName(Property.of("events"))
            .build();

        RunContext runContext = createMockRunContext(null);
        TableIdentifier identifier = task.testTableIdentifier(runContext);

        assertThat(identifier.namespace().levels(), arrayContaining("analytics"));
        assertThat(identifier.name(), is("events"));
    }

    @Test
    void shouldParseTableIdentifierMultiLevel() throws Exception {
        DummyIcebergTableTask task = DummyIcebergTableTask.builder()
            .namespace(Property.of("analytics.raw.v1"))
            .tableName(Property.of("pageviews"))
            .build();

        RunContext runContext = createMockRunContext(null);
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
        public java.util.List<TableIdentifier> listTables(org.apache.iceberg.catalog.Namespace namespace) {
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
        public org.apache.iceberg.catalog.Catalog.TableBuilder buildTable(TableIdentifier identifier, org.apache.iceberg.Schema schema) {
            return null;
        }

        @Override
        public void initialize(String name, Map<String, String> properties) {}
    }
}
