package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (!Files.exists(context.currentSegmentContext().getSegmentPath())) {
            throw new DatabaseException("Can not open path " +
                    context.currentSegmentContext().getSegmentPath().toString());
        }
        Path workFileName = context.currentSegmentContext().getSegmentPath();
        if (!Files.exists(workFileName)) {
            throw new DatabaseException("Can not exists " + workFileName);
        }
        try (var fileStream = new FileInputStream(workFileName.toString());
             var dbStream = new DatabaseInputStream(fileStream)) {
            Optional<DatabaseRecord> dbRecord = dbStream.readDbUnit();
            Set<String> keysSet = new HashSet<>();
            while (dbRecord.isPresent()) {
                String key = new String(dbRecord.get().getKey());
                keysSet.add(key);
                // + dbRecord.size ?
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(
                        key, new SegmentOffsetInfoImpl(context.currentSegmentContext().getCurrentSize()));
                context = InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(context.currentDbContext())
                        .currentTableContext(context.currentTableContext())
                        .currentSegmentContext(new SegmentInitializationContextImpl(
                                context.currentSegmentContext().getSegmentName(),
                                context.currentSegmentContext().getSegmentPath(),
                                context.currentSegmentContext().getCurrentSize() + dbRecord.get().size(),
                                context.currentSegmentContext().getIndex()))
                        .build();
                try {
                    dbRecord = dbStream.readDbUnit();
                }
                catch(IOException e){
                    dbRecord = Optional.empty();
                }
            }
            Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
            context.currentTableContext().updateCurrentSegment(segment);
            for (String key : keysSet){
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
            }
        } catch (IOException e) {
            throw new DatabaseException("Problems with reading from segment", e);
        }
    }
}
