package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.io.File;
import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path dbPath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.dbPath = databasePath;
        this.tableIndex = tableIndex;
    }

    @Override
    public String getTableName() {

        return tableName;
    }

    @Override
    public Path getTablePath() {

        return Path.of(dbPath.toString() + File.separator + tableName);
    }

    @Override
    public TableIndex getTableIndex() {

        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {

        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {

        currentSegment = segment;
    }
}
