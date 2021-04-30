package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public class DatabaseServerInitializer implements Initializer {

    private final DatabaseInitializer dbInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        dbInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workPath = context.executionEnvironment().getWorkingPath();

        if (!Files.exists(workPath)) {
            try {
                Files.createDirectory(workPath);
            } catch (IOException e) {
                throw new DatabaseException("Failed to create directory " + workPath, e);
            }
        }

        for (final File fileEntry : Objects.requireNonNull(workPath.toFile().listFiles())) {
            if (!Files.exists(fileEntry.toPath())) {
                throw new DatabaseException("Can not open potential database with name " + fileEntry);
            }
            Optional<Database> db = context.executionEnvironment().getDatabase(fileEntry.getName());
            // добавляем уже насыщуенню дб в DatabaseInitializer
            // if (db.isEmpty()) {context.executionEnvironment().addDatabase(DatabaseImpl.create(fileEntry.getName(),
            // workPath));}
            InitializationContext newContext = InitializationContextImpl.builder().currentDatabaseContext(
                    new DatabaseInitializationContextImpl(fileEntry.getName(), workPath)).executionEnvironment(
                    context.executionEnvironment()).build();
            dbInitializer.perform(newContext);
        }
    }
}
