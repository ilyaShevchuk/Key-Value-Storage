package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    DatabaseCache cache;
    private Table table;

    public CachingTable(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        try {
            table = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        } catch (IOException e) {
            throw new DatabaseException("Failed to create CachingTable ", e);
        }
        cache = new DatabaseCacheImpl(5000);
    }

    @Override
    public String getName() {

        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var value = cache.get(objectKey);
        if (value == null) {
            return table.read(objectKey);
        } else {
            return Optional.of(value);
        }
    }

    @Override
    public void delete(String objectKey) {
        cache.delete(objectKey);
    }
}
