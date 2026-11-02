package io.kestra.plugin.iceberg;

import org.apache.iceberg.catalog.Catalog;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

public final class TestCatalogs {

    private TestCatalogs() {}

    public static <T extends AbstractIcebergTask> T withCatalog(T task, Catalog catalog) {
        var spy = Mockito.spy(task);
        Mockito.doReturn(catalog).when(spy).buildCatalog(any(), any());
        return spy;
    }
}
