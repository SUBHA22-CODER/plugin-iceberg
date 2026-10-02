package io.kestra.plugin.iceberg;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;
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
}
