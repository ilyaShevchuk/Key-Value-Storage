package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private long currentSize;
    private Path tablePath;
    private SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {

        this.segmentName = segmentName;
        this.tablePath = tablePath;
        this.segmentPath = Paths.get(tablePath.toString() + File.separator + segmentName);
        this.currentSize = currentSize;
        this.index = new SegmentIndex();
    }

    @Override
    public String getSegmentName() {

        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {

        return index;
    }

    @Override
    public long getCurrentSize() {

        return currentSize;
    }

    public void updateCurrentSize(int x ){
        currentSize += x;
    }
}
