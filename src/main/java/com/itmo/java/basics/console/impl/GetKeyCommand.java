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
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {
    private static final int RIGHT_COUNT_OF_ARGS = 5;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() != RIGHT_COUNT_OF_ARGS) {
            throw new IllegalArgumentException(String.format("Count of Args is wrong, expected %d, received %d",
                    RIGHT_COUNT_OF_ARGS, commandArgs.size()));
        }
        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        Optional<Database> database = env.getDatabase(dbName);
        Optional<byte[]> readValue;
        if (database.isEmpty()) {
            return DatabaseCommandResult.error(String.format("Database %s n not exists", dbName));
        }
        try {
            readValue = database.get().read(commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString());

        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(readValue.orElse(null));
    }
}
