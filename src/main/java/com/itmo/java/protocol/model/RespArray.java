package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private final RespObject[] objects;

    public RespArray(RespObject... objects) {
        this.objects = objects;
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

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringBuilder result = new StringBuilder();
        for (RespObject object : objects) {
            result.append(object.asString());
            result.append(" ");
        }
        return String.valueOf(result.delete(result.length() - 2, result.length() - 1));
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(objects.length).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
        for (RespObject object : objects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return Arrays.asList(objects);
    }
}
