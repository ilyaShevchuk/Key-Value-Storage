package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workPath = context.currentTableContext().getTablePath();
        if (!Files.exists(context.currentTableContext().getTablePath())) {
            throw new DatabaseException("Wrong path to table " + context.currentTableContext().getTableName());
        }
        ArrayList<String> segmentList = new ArrayList<>();
        for (final File fileEntry : Objects.requireNonNull(workPath.toFile().listFiles())) {
            segmentList.add(fileEntry.getName());
        }
        Collections.sort(segmentList);
        for (final String currentSegmentName : segmentList) {
            segmentInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(currentSegmentName,
                            context.currentTableContext().getTablePath(), 0)).build());
        }
        Table table = TableImpl.initializeFromContext(context.currentTableContext());
        context.currentDbContext().addTable(table);

    }
}
