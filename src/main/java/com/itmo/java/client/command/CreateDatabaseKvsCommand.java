package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";
    private final String databaseName;
    private final int id;
    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        this.databaseName = databaseName;
        this.id = idGen.get();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        RespObject[] objects = {new RespCommandId(id),
                new RespBulkString(COMMAND_NAME.getBytes()),
                new RespBulkString(databaseName.getBytes())};
        return new RespArray(objects);
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
