package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
            } catch (IOException e) {
                throw new DatabaseException("Failed to create directory " + workPath, e);
            }
        }

        try (final Stream<Path> walk = Files.walk(workPath, 1)) {
            List<Path> databasesPath = walk.filter(Files::isDirectory).collect(Collectors.toList());
            databasesPath.remove(0);

            for (final Path databasePath : databasesPath) {
                final DatabaseInitializationContextImpl databaseInitializationContext
                        = new DatabaseInitializationContextImpl(databasePath.getFileName().toString(), workPath);

                dbInitializer.perform(
                        new InitializationContextImpl(
                                context.executionEnvironment(), databaseInitializationContext,
                                context.currentTableContext(), context.currentSegmentContext()
                        )
                );
            }
        } catch (IOException exception) {
            throw new DatabaseException("Working directory walking error: " + workPath, exception);
        }
    }
}
