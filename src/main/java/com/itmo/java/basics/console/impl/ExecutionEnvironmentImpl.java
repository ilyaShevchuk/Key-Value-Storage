package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final String workingPath;
    private static final Map<String, Database> databaseMap = new HashMap<>();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.workingPath = config.getWorkingPath();
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        if (!databaseMap.containsKey(name)){
            return Optional.empty();
        }
        else {
            return Optional.of(databaseMap.get(name));
        }
    }

    @Override
    public void addDatabase(Database db) {
        databaseMap.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Paths.get(workingPath);
    }
}
