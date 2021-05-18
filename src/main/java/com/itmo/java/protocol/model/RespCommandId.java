package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Id
 */
public class RespCommandId implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '!';

    private Integer ID;

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
        os.write(ID.toString().getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
    }
}
