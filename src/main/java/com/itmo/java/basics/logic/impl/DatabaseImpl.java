package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private static final Map<String, Table> tablesMap = new HashMap<>();
    private final String name;
    private final Path baseDir;

    private DatabaseImpl(String dbName, Path databaseRoot) throws IOException {
        name = dbName;
        baseDir = Paths.get(databaseRoot.toString() + File.separator + dbName);
        Files.createDirectory(baseDir);
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("DbName is null");
        }
        try {
            return new DatabaseImpl(dbName, databaseRoot);
        } catch (IOException e) {
            throw new DatabaseException("Failed to create database ", e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        Table table;
        try {
            if (tableName == null) {
                throw new DatabaseException("Table name is null");
            }
            table = TableImpl.create(tableName, baseDir, new TableIndex());

        } catch (DatabaseException e) {
            throw new DatabaseException("Can not create table with name " + tableName, e);
        }
        tablesMap.put(tableName, table);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tablesMap.containsKey(tableName)) {
            throw new DatabaseException("There are not table with name" + tableName);
        }
        tablesMap.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!tablesMap.containsKey(tableName)) {
            throw new DatabaseException("There are not table with name" + tableName);
        }
        return tablesMap.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tablesMap.containsKey(tableName)) {
            throw new DatabaseException("There are not table with name" + tableName);
        }
        tablesMap.get(tableName).delete(objectKey);

    }
}
