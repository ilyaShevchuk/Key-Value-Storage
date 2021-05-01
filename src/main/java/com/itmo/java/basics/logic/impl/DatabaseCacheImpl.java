package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;


public class DatabaseCacheImpl implements DatabaseCache {

    private final Map<String, byte[]> cacheMap;

    public DatabaseCacheImpl(Integer n) {
        cacheMap = new LinkedHashMap<>(n, 0.75f, true){
            protected boolean removeEldestEntry(Map.Entry eldest){ return size() > n;}
        };
    }

    @Override
    public byte[] get(String key) {
        return cacheMap.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cacheMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        cacheMap.remove(key);
    }
}
