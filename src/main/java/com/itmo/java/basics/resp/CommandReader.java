package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommandReader implements AutoCloseable {
    private final RespReader reader;
    private final ExecutionEnvironment env;


    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
//        RespObject object = reader.readObject();
//        if (object instanceof RespArray) {
//            List<RespObject> objects = ((RespArray) object).getObjects();
//            if (objects.get(0) instanceof RespCommandId){
//                return true;
//            }
//        }
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
//        if (!hasNextCommand()){
//            throw new IOException("We dont have any command");
//        }
        RespArray args = reader.readArray();
        if (args.getObjects().size() < 2){
            throw new IllegalArgumentException("Not enough args for command");
        }
        RespObject commandName = args.getObjects().get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        return DatabaseCommands.valueOf(commandName.asString()).getCommand(env, args.getObjects());

    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
