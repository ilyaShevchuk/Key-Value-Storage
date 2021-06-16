package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private final InputStream is;

    private byte readByte() throws IOException {
        final int symbol = is.read();

        if (symbol == -1) {
            throw new EOFException("End of stream");
        }

        return (byte) symbol;
    }

    private int readInt() throws IOException {
        String textCount = "";
        while (true) {
            byte x1 = readByte();
            if (!Character.isDigit(x1) && (char)x1 != '-') {
                break;
            }
            textCount += (char)x1;
        }
        return Integer.parseInt(textCount);
    }

    public RespReader(InputStream is) {
        this.is = is;
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
        char objectType = (char) is.read();
        switch (objectType) {
            case ('*'):
                return readArray();
            case ('-'):
                return readError();
            case ('$'):
                return readBulkString();
            case ('!'):
                return readCommandId();
            default:
                throw new IOException("No such RESP object code like that" + objectType);
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        //Skip bytes for CR an LF
        String errorText = "";
        while (true) {
            int currentByte = is.read();
            if (currentByte == -1){
                throw new EOFException("Can not read more");
            }
            if ((char)currentByte == '\\') {
                String threeNextBytes = new String(is.readNBytes(3));
                if (threeNextBytes.length() != 3){
                    throw new EOFException("Can not read more from " + is.toString());
                }
                if (threeNextBytes.equals("r\\n")) {
                    break;
                }
                errorText += threeNextBytes;
                break;
            }
            errorText += (char)currentByte;
        }
        return new RespError(errorText.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString () throws IOException {
        int count = readInt();
        //Skip bytes for CR an LF
        //is.skipNBytes(3);
        long skipped = is.skip(4);
        if (count == -1){
            return RespBulkString.NULL_STRING;
        }
        byte[] bulkString = is.readNBytes(count);
        if (bulkString.length != count){
            throw new EOFException("Can not read more than " + new String(bulkString));
        }
        return new RespBulkString(bulkString);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray () throws IOException {
        int count = readInt();
        //is.skipNBytes(4);
        long skipped = is.skip(4);
        ArrayList<RespObject> objects = new ArrayList<>();
        for (int i=0;i < count;i++){
            objects.add(readObject());
        }
        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId () throws IOException {
        int commandId = readInt();
        //is.skipNBytes(4);
        long skipped = is.skip(4);
        return new RespCommandId(commandId);
    }


    @Override
    public void close () throws IOException {
        is.close();
    }
}
