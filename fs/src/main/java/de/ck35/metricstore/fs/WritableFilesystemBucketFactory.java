package de.ck35.metricstore.fs;

import java.nio.file.Path;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import de.ck35.metricstore.util.LRUCache;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

/**
 * Factory for {@link WritableFilesystemBucket}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class WritableFilesystemBucketFactory implements Function<BucketData, WritableFilesystemBucket> {

    private final Function<ObjectNode, DateTime> timestampFunction;
    private final Function<Path, ObjectNodeWriter> writerFactory;
    private final Function<Path, ObjectNodeReader> readerFactory;
    private final Supplier<Integer> maxCachedWritersSetting;

    public WritableFilesystemBucketFactory(Function<ObjectNode, DateTime> timestampFunction,
                                           Function<Path, ObjectNodeWriter> writerFactory,
                                           Function<Path, ObjectNodeReader> readerFactory,
                                           Supplier<Integer> maxCachedWritersSetting) {
        this.timestampFunction = timestampFunction;
        this.writerFactory = writerFactory;
        this.readerFactory = readerFactory;
        this.maxCachedWritersSetting = maxCachedWritersSetting;
    }

    @Override
    public WritableFilesystemBucket apply(BucketData input) {
        if(input == null) {
            return null;
        }
        return new WritableFilesystemBucket(input, 
                                            timestampFunction, 
                                            writerFactory, 
                                            readerFactory, 
                                            writersLRUCache());
    }
    
    protected LRUCache<Path, ObjectNodeWriter> writersLRUCache() {
        return new LRUCache<Path, ObjectNodeWriter>(maxCachedWritersSetting);
    }
}