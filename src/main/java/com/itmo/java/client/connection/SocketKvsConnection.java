package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final Socket socket;
    private final RespWriter respWriter;
    private final RespReader respReader;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            socket = new Socket(config.getHost(), config.getPort());
            respWriter = new RespWriter(socket.getOutputStream());
            respReader = new RespReader(socket.getInputStream());
        } catch (IOException e) {
            throw new RuntimeException("Creation socket error", e);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        if (socket.isClosed()) {
            throw new ConnectionException("Socket is closed");
        }

        try {
            respWriter.write(command);
            return respReader.readObject();
        } catch (IOException e) {
            throw new ConnectionException("Write/read connection exception", e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            respWriter.close();
            respReader.close();
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error when try to close Writer, reader, socket", e);
        }
    }
}
