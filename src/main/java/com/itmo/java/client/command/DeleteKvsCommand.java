package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

public class DeleteKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "DELETE_KEY";
    private final String tableName;
    private final String databaseName;
    private final String key;
    private final int id;


    public DeleteKvsCommand(String databaseName, String tableName, String key) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
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
                new RespBulkString(databaseName.getBytes()),
                new RespBulkString(tableName.getBytes()),
                new RespBulkString(key.getBytes())};
        return null;
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
