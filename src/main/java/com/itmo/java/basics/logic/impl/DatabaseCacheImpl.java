package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;


public class DatabaseCacheImpl implements DatabaseCache {

    private final Map<String, byte[]> hashMap;

    public DatabaseCacheImpl(Integer n) {
        hashMap = new LinkedHashMap<>(n, 0.75f, true);
    }

    @Override
    public byte[] get(String key) {
        return hashMap.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        hashMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        hashMap.remove(key);
    }
}
