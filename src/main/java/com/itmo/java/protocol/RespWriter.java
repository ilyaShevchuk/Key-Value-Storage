package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespWriter implements AutoCloseable{

    private OutputStream os;

    public RespWriter(OutputStream os) {
        this.os = os;
    }

    /**
     * Записывает в output stream объект
     */
    public void write(RespObject object) throws IOException {
        os.write(object.asString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        os.close();
    }
}
