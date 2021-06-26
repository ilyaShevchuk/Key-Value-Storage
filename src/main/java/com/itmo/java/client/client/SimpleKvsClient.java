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

    private RespObject generateExceptionByCommand(KvsCommand command, String exceptionMessage)
            throws DatabaseExecutionException {
        try {
            RespObject result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException(result.asString());
            }
            return result;
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(exceptionMessage, e);
        }
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        final String excMessage = String.format("Failed to create-database Command in %s " +
                "with KvsConnection", dbName);
        return generateExceptionByCommand(new CreateDatabaseKvsCommand(dbName), excMessage).asString();
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        final String excMessage = String.format("Failed to create-table Command %s in database %s " +
                "with KvsConnection", tableName, dbName);
        return generateExceptionByCommand(new CreateTableKvsCommand(dbName, tableName), excMessage).asString();
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        final String excMessage = String.format("Failed to get key %s Command in %s in database %s " +
                "with KvsConnection", key, tableName, dbName);
        return generateExceptionByCommand(new GetKvsCommand(dbName, tableName, key), excMessage).asString();
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        final String excMessage = String.format("Failed to set key %s and value %s Command in %s " +
                "in database %s with KvsConnection", key, value, tableName, dbName);
        return generateExceptionByCommand(new SetKvsCommand(dbName, tableName, key, value), excMessage).asString();
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        final String excMessage = String.format("Failed to delete key %s Command in %s in database %s " +
                "with KvsConnection", key, tableName, dbName);
        return generateExceptionByCommand(new DeleteKvsCommand(dbName, tableName, key), excMessage).asString();

    }
}
