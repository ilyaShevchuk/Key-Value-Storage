package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {

        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        String workPath = initialContext.currentDbContext().getDatabasePath().toString();
        if (!Files.exists(initialContext.currentDbContext().getDatabasePath())) {
            throw new DatabaseException("Wrong path to database - "
                    + initialContext.currentDbContext().getDatabasePath());
        }
        for (final File fileEntry : Objects.requireNonNull(new File(workPath).listFiles())) {
            if (!Files.exists(fileEntry.toPath())) {
                throw new DatabaseException("Can not open potential table with name - " + fileEntry.getName());
            }
            TableIndex tableIndex = new TableIndex();
            InitializationContext newContext = InitializationContextImpl.builder()
                    .executionEnvironment(initialContext.executionEnvironment())
                    .currentDatabaseContext(initialContext.currentDbContext())
                    .currentTableContext(new TableInitializationContextImpl(fileEntry.getName(),
                            initialContext.currentDbContext().getDatabasePath(), tableIndex)).build();
            tableInitializer.perform(newContext);
        }
        Database db = DatabaseImpl.initializeFromContext(initialContext.currentDbContext());
        initialContext.executionEnvironment().addDatabase(db);
    }
}
