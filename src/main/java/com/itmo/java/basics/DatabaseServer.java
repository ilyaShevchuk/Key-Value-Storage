package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespArray;
import lombok.Builder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Builder
public class DatabaseServer {

    private final ExecutionEnvironment serverEnv;
    private InitializationContext context;
    private final DatabaseServerInitializer initializer;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private DatabaseServer(ExecutionEnvironment env, DatabaseServerInitializer initializer){
        this.serverEnv = env;
        this.initializer = initializer;
    }

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */

    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        InitializationContext context = InitializationContextImpl.builder().executionEnvironment(env).build();
        initializer.perform(context);
        return new DatabaseServer(env, initializer);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            DatabaseCommand command = DatabaseCommands.valueOf(message.getObjects().get(
                    DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString()).getCommand(serverEnv,
                    message.getObjects());

            return command.execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }
}