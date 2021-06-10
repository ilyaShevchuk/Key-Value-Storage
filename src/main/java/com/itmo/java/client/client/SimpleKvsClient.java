package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String dbName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.dbName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand command = new CreateDatabaseKvsCommand(dbName);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Failed to create-database Command in %s " +
                    "with KvsConnection", dbName), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand command = new CreateTableKvsCommand(dbName, tableName);
        RespObject result;
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Failed to create-table Command %s in database %s " +
                    "with KvsConnection", tableName, dbName), e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new GetKvsCommand(dbName, tableName, key);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Failed to get key %s Command in %s in database %s " +
                    "with KvsConnection", key, tableName, dbName), e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand command = new SetKvsCommand(dbName, tableName, key, value);
        RespObject result;
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Failed to set key %s and value %s Command in %s " +
                    "in database %s with KvsConnection", key, value, tableName, dbName), e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Failed to delete key %s Command in %s in database %s " +
                    "with KvsConnection", key, tableName, dbName), e);
        }
    }
}
