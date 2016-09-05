package de.ck35.metricstore.cache.core.buckets;

import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Function;
import com.google.common.base.Supplier;

import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

public class MinuteBucketSupplier implements Supplier<MinuteBucket> {

    private final ExpandedBucketManager expandedBucketManager;
    private final Function<InputStream, ObjectNodeReader> objectNodeReaderFactory;
    private final Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory;
    
    public MinuteBucketSupplier(ExpandedBucketManager expandedBucketManager,
                                Function<InputStream, ObjectNodeReader> objectNodeReaderFactory,
                                Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory) {
        this.expandedBucketManager = expandedBucketManager;
        this.objectNodeReaderFactory = objectNodeReaderFactory;
        this.objectNodeWriterFactory = objectNodeWriterFactory;
    }

    @Override
    public MinuteBucket get() {
        return new MinuteBucket(objectNodeReaderFactory, objectNodeWriterFactory, expandedBucketManager);
    }
    
}