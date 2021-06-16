package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final DatabaseServer databaseServer;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;
        this.serverSocket = new ServerSocket(config.getPort());
    }

    public static void main(String[] args) throws Exception {
        // можнно запускать прямо здесь
        ConfigLoader configLoader = new ConfigLoader();
        DatabaseServerConfig config = configLoader.readConfig();
        DatabaseServer dbServer = DatabaseServer.initialize(new ExecutionEnvironmentImpl(config.getDbConfig()),
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
        JavaSocketServerConnector connector = new JavaSocketServerConnector(dbServer, config.getServerConfig());
        connector.start();
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    ClientTask task = new ClientTask(socket, databaseServer);
                    clientIOWorkers.submit(task);
                } catch (IOException e) {
                    System.out.println("Error on server while accepting " + e.getMessage());
                }
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        try {
            serverSocket.close();
            connectionAcceptorExecutor.shutdownNow();
            clientIOWorkers.shutdownNow();
        } catch (IOException e) {
            throw new RuntimeException("Can not close server socket" + serverSocket.toString(), e);
        }
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        private final Socket clientSocket;
        private final DatabaseServer server;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.clientSocket = client;
            this.server = server;
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try {
                CommandReader reader = new CommandReader(new RespReader(clientSocket.getInputStream()), server.getEnv());
                RespWriter writer = new RespWriter(clientSocket.getOutputStream());
                while (reader.hasNextCommand()) {
                    DatabaseCommandResult result = server.executeNextCommand(reader.readCommand()).get();
                    writer.write(result.serialize());
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                clientSocket.close();
            } catch (IOException e) {
                throw new RuntimeException("Can not close client socket" + clientSocket.toString(), e);
            }
        }
    }
}
