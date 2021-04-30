package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> tableMap;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        tableMap = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Path getDatabasePath() {
        return Path.of(databaseRoot.toString() + File.separator + dbName);
    }

    @Override
    public Map<String, Table> getTables() {

        return tableMap;
    }

    @Override
    public void addTable(Table table) {
        if (!tableMap.containsKey(table.getName())) {
            tableMap.put(table.getName(), table);
        }
        //else {}
    }
}
