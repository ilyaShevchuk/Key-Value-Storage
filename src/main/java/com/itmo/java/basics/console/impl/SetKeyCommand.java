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
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {
    private static final int RIGHT_COUNT_OF_ARGS = 6;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() != RIGHT_COUNT_OF_ARGS) {
            throw new IllegalArgumentException(String.format("Count of Args is wrong, expected %d, received %d",
                    RIGHT_COUNT_OF_ARGS, commandArgs.size()));
        }
        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
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
            prev = database.get().read(
                    commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString());
            database.get().write(
                    commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString().getBytes());
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(prev.orElse(null));
    }
}
