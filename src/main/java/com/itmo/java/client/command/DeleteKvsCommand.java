package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

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
        RespObject[] objects = {
                new RespCommandId(id),
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tableName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8))};
        return new RespArray(objects);
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
