package com.itmo.java.basics.logic.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class lfsMap<K, V> extends LinkedHashMap<K, V> {

    private final int capacity;

    public lfsMap(int capacity) {
        super(capacity, 1f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest){
        return size() > capacity;
    }
}
