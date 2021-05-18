package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

public class SetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "SET_KEY";
    private final String tableName;
    private final String databaseName;
    private final String key;
    private final String value;
    private final int id;

    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.key = key;
        this.value = value;
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
                new RespBulkString(key.getBytes()),
                new RespBulkString(value.getBytes())};
        return new RespArray(objects);
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
