package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

/**
 * Зафейленная команда
 */
public class FailedDatabaseCommandResult implements DatabaseCommandResult {

    private final String message;

    public FailedDatabaseCommandResult(String payload) {
        message = payload;
    }

    /**
     * Сообщение об ошибке
     */
    @Override
    public String getPayLoad() {
        return message;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    /**
     * Сериализуется в {@link RespError}
     */
    @Override
    public RespObject serialize() {
        return new RespBulkString(message.getBytes());
    }
}
