package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                return;
            } catch (IOException e) {
                throw new DatabaseException("Failed to create directory " + workPath, e);
            }
        }
        File[] dbDirectories = workPath.toFile().listFiles();
        if (dbDirectories == null){
            throw new DatabaseException(String.format("Can not formed files list from %s", workPath));
        }
        for (final File dbDirectory : dbDirectories){
            if (!Files.isDirectory(dbDirectory.toPath())){
                continue;
            }
            dbInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(new DatabaseInitializationContextImpl(
                            dbDirectory.getName(), workPath))
                    .build());
        }
    }
}
