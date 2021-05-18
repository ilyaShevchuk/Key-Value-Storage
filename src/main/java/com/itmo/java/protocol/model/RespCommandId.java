package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Id
 */
public class RespCommandId implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '!';

    private final Integer ID;

    public RespCommandId(int commandId) {
        ID = commandId;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return String.valueOf(ID);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(ID);
        os.write(byteBuffer.array());
        os.write(CRLF);
    }
}
