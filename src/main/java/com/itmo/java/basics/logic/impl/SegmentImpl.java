package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private static final int maxSize = 100000;
    private final Path pathForWriting;
    private final SegmentIndex indexMap;
    private long offset;

    private SegmentImpl(Path segPath) {
        indexMap = new SegmentIndex();
        pathForWriting = segPath;
        offset = 0;
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        try {
            return new SegmentImpl(Files.createFile(Path.of(tableRootPath.toString() + File.separator
                    + segmentName)));
        } catch (IOException e) {
            throw new DatabaseException("Segment create fail", e);
        }
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return pathForWriting.getFileName().toString();
    }

    private int writeWithStream(WritableDatabaseRecord record) throws IOException {
        try (DatabaseOutputStream stream = new DatabaseOutputStream(new FileOutputStream(pathForWriting.toString(), true))) {
            indexMap.onIndexedEntityUpdated(new String(record.getKey()), new SegmentOffsetInfoImpl(offset));
            return stream.write(record);
        }
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        WritableDatabaseRecord record;
        if (objectValue == null) {
            record = new RemoveDatabaseRecord(objectKey.getBytes());
        } else {
            record = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        }
        if (isReadOnly()) {
            return false;
        }
        offset += writeWithStream(record);
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var currentOffset = indexMap.searchForKey(objectKey);
        if (currentOffset.isEmpty()) {
            return Optional.empty();
        }
        try (var fileStream = new FileInputStream(pathForWriting.toString())) {
            long skipped = fileStream.skip(currentOffset.get().getOffset());
            if (skipped != currentOffset.get().getOffset()) {
                throw new IOException("File(Segment) problems: Can't skip offset bytes");
            }
            try (var stream = new DatabaseInputStream(fileStream)) {
                var readRecord = stream.readDbUnit();
                return readRecord.map(DatabaseRecord::getValue);
            }
        }
    }

    @Override
    public boolean isReadOnly() {
        return pathForWriting.toFile().length() >= maxSize;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (isReadOnly()) {
            return false;
        }
        writeWithStream(new RemoveDatabaseRecord(objectKey.getBytes()));
        return true;
    }

}
