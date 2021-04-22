package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String name;
    private final Path tableRoot;
    private final TableIndex tableIndex;
    private final Map<String, Segment> segments;
    private Segment lastSegment = null;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tbIndex) throws IOException {
        name = tableName;
        tableIndex = tbIndex;
        segments = new HashMap<String, Segment>();
        Path dir = Paths.get(pathToDatabaseRoot.toString() + File.separator + tableName);
        Files.createDirectory(dir);
        tableRoot = dir;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        try {
            return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        } catch (IOException e) {
            throw new DatabaseException("Can not create table", e);
        }
    }

    @Override
    public String getName() {

        return name;
    }

    public void makeSegmentAvailable() throws DatabaseException {
        if (lastSegment == null || lastSegment.isReadOnly()) {
            lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), tableRoot);
            segments.put(lastSegment.getName(), lastSegment);
        }
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        makeSegmentAvailable();
        try {
            lastSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);

        } catch (IOException e) {
            throw new DatabaseException("Can not write to Table " + name, e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) {
            return Optional.empty();
        }
        try {
            return segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Can not read from segment with name " + segment.get().getName(), e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        makeSegmentAvailable();
        try {
            lastSegment.delete(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Can not add info about delete on segment with name "
                    + lastSegment.getName(), e);
        }
    }
}
