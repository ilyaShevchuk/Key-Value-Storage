package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {
    private static final int RIGHT_COUNT_OF_ARGS = 4;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() != RIGHT_COUNT_OF_ARGS) {
            throw new IllegalArgumentException("Count of Args is wrong");
        }
        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        Optional<Database> database = env.getDatabase(dbName);
        Optional<byte[]> prev;
        if (database.isEmpty()) {
            return DatabaseCommandResult.error(String.format("Database %s n not exists", dbName));
        }
        try {
            database.get().createTableIfNotExists(
                    commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString());
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(String.format("Table %s was created in database %s", dbName,
                commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString()).getBytes());

    }
}
