package io.kestra.plugin.iceberg.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared in-memory catalog for testing multi-task workflows within the same JVM process.
 * Holds static state across separate task catalog(runContext) initializations.
 */
public class SharedInMemoryCatalog implements Catalog, SupportsNamespaces, Closeable {

    private static final InMemoryCatalog DELEGATE = new InMemoryCatalog();
    private static volatile boolean initialized = false;

    public static synchronized void reset() {
        // Drop all namespaces and tables
        try {
            for (Namespace ns : DELEGATE.listNamespaces()) {
                for (TableIdentifier table : DELEGATE.listTables(ns)) {
                    DELEGATE.dropTable(table, false);
                }
                DELEGATE.dropNamespace(ns);
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        if (!initialized) {
            synchronized (SharedInMemoryCatalog.class) {
                if (!initialized) {
                    DELEGATE.initialize(name, properties);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public String name() {
        return DELEGATE.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return DELEGATE.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return DELEGATE.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        DELEGATE.renameTable(from, to);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        return DELEGATE.loadTable(identifier);
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
        return DELEGATE.registerTable(identifier, metadataFileLocation);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        return DELEGATE.buildTable(identifier, schema);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        return DELEGATE.tableExists(identifier);
    }

    @Override
    public void createNamespace(Namespace namespace) {
        DELEGATE.createNamespace(namespace);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        DELEGATE.createNamespace(namespace, metadata);
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
        return DELEGATE.namespaceExists(namespace);
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        return DELEGATE.dropNamespace(namespace);
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        return DELEGATE.setProperties(namespace, properties);
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        return DELEGATE.removeProperties(namespace, properties);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        return DELEGATE.loadNamespaceMetadata(namespace);
    }

    @Override
    public List<Namespace> listNamespaces() {
        return DELEGATE.listNamespaces();
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        return DELEGATE.listNamespaces(namespace);
    }

    @Override
    public void close() throws IOException {
        // Do not close the shared delegate between task executions
    }
}
