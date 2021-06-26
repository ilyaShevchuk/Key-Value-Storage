package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private final InputStream is;

    public RespReader(InputStream is) {
        this.is = is;
    }

    private byte readByte() throws IOException {
        final int symbol = is.read();

        if (symbol == -1) {
            throw new EOFException("End of stream");
        }

        return (byte) symbol;
    }

    private void skipCR() throws IOException {
        if (readByte() != CR) {
            throw new IOException("Wrong format");
        }
    }

    private void skipLF() throws IOException {
        if (readByte() != LF) {
            throw new IOException("Wrong format");
        }
    }

    private byte[] readBytesForInt() throws IOException {
        byte symbol = readByte();
        final List<Byte> symbols = new ArrayList<>();
        while (symbol != CR) {
            symbols.add(symbol);
            symbol = readByte();
        }

        final int symbolsCount = symbols.size();
        final byte[] bytes = new byte[symbolsCount];
        for (int i = 0; i < symbolsCount; i++) {
            bytes[i] = symbols.get(i);
        }

        return bytes;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        int code = is.read();
        return (code == RespArray.CODE);
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        switch (readByte()) {
            case RespError.CODE:
                return readError();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            case RespCommandId.CODE:
                return readCommandId();
            default:
                throw new IOException("Invalid object code");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        byte symbol = readByte();
        List<Byte> symbols = new ArrayList<>();
        boolean textEnd = false;
        while (!textEnd) {
            while (symbol == CR) {

                symbol = readByte();

                if (symbol == LF) {
                    textEnd = true;
                } else {
                    symbols.add(CR);
                }
            }
            if (!textEnd) {
                symbols.add(symbol);
                symbol = readByte();
            }
        }
        return new RespError(symbols);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        int count = Integer.parseInt(new String(readBytesForInt(), StandardCharsets.UTF_8));
        skipLF();
        if (count == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }
        final byte[] data = is.readNBytes(count);
        if (data.length != count) {
            throw new EOFException("End of stream");
        }
        skipCR();
        skipLF();
        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        int size = Integer.parseInt(new String(readBytesForInt(), StandardCharsets.UTF_8));
        skipLF();
        RespObject[] objects = new RespObject[size];
        for (int i = 0; i < size; i++) {
            objects[i] = readObject();
        }
        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        int intSize = 4;
        byte[] bytes = is.readNBytes(intSize);
        if (bytes.length != intSize) {
            throw new EOFException("End of stream");
        }
        int commandId = ByteBuffer.wrap(bytes).getInt();
        skipCR();
        skipLF();
        return new RespCommandId(commandId);
    }


    @Override
    public void close() throws IOException {
        is.close();
    }
}
