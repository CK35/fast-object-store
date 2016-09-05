package de.ck35.metricstore.cache.core.buckets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import de.ck35.metricstore.util.io.MetricsIOException;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

public class MinuteBucket implements Iterable<ObjectNode> {

    private final Function<InputStream, ObjectNodeReader> objectNodeReaderFactory;
    private final Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory;
    
    private final BucketExpandListener expandListener;

    private final ReadWriteLock lock;
    private ExpandedBucket expandedBucket;
    private CompressedBucket compressedBucket;
    
    public MinuteBucket(Function<InputStream, ObjectNodeReader> objectNodeReaderFactory,
                        Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory,
                        BucketExpandListener expandListener) {
        this.objectNodeReaderFactory = objectNodeReaderFactory;
        this.objectNodeWriterFactory = objectNodeWriterFactory;
        this.expandListener = expandListener;
        this.lock = new ReentrantReadWriteLock();
        this.expandedBucket = null;
        this.compressedBucket = null;
    }
    
    @Override
    public Iterator<ObjectNode> iterator() {
        this.lock.readLock().lock();
        try {
            if(compressedBucket != null) {
                return compressedBucket.iterator();
            }
            if(expandedBucket != null) {
                return expandedBucket.iterator();
            }
            return Collections.<ObjectNode>emptySet().iterator();
        } finally {
            this.lock.readLock().unlock();
        }
    }
    
    public void write(ObjectNode node) {
        boolean expanded;
        this.lock.writeLock().lock();
        try {
            if(expandedBucket == null) {
                expanded = true;
                expandedBucket = new ExpandedBucket(compressedBucket);
                compressedBucket = null;
            } else {
                expanded = false;
            }
            expandedBucket.add(node);
        } finally {
            this.lock.writeLock().unlock();
        }
        if(expanded) {    
            expandListener.expanded(this);
        }
    }
    
    public void compress() {
        this.lock.writeLock().lock();
        try {
            if(compressedBucket == null && expandedBucket != null) {
                compressedBucket = CompressedBucket.build(expandedBucket, objectNodeReaderFactory, objectNodeWriterFactory);
                expandedBucket = null;
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }
    
    public boolean isCompressed() {
        this.lock.readLock().lock();
        try {
            return expandedBucket == null;
        } finally {
            this.lock.readLock().unlock();
        }
    }
    
    public long getSize() {
        this.lock.readLock().lock();
        try {
            if(expandedBucket != null) {
                return expandedBucket.getNodeCount();
            }
            if(compressedBucket != null) {
                return compressedBucket.getNodeCount();
            }
            return 0;
        } finally {
            this.lock.readLock().unlock();
        }
    }
    
    public static class CompressedBucket implements Iterable<ObjectNode> {
        
        private final byte[] bytes;
        private final int nodeCount;
        private final Function<InputStream, ObjectNodeReader> objectNodeReaderFactory;
        
        public CompressedBucket(byte[] bytes, int nodeCount, Function<InputStream, ObjectNodeReader> objectNodeReaderFactory) {
            this.bytes = bytes;
            this.nodeCount = nodeCount;
            this.objectNodeReaderFactory = objectNodeReaderFactory;
        }
        public static CompressedBucket build(Iterable<ObjectNode> nodes,
                                             Function<InputStream, ObjectNodeReader> objectNodeReaderFactory,
                                             Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            int nodeCount = 0;
            try(ObjectNodeWriter writer = objectNodeWriterFactory.apply(bytes)) {
                for(ObjectNode node : nodes) {
                    writer.write(node);
                    nodeCount++;
                }
            } catch (IOException e) {
                throw new MetricsIOException("Could not create compressed minute bucket!", e);
            }
            return new CompressedBucket(bytes.toByteArray(), nodeCount, objectNodeReaderFactory);
        }
        public int getNodeCount() {
            return nodeCount;
        }
        @Override
        public Iterator<ObjectNode> iterator() {
            return new CompressedBucketIterator(objectNodeReaderFactory.apply(new ByteArrayInputStream(bytes)));
        }
        public static class CompressedBucketIterator extends AbstractIterator<ObjectNode> {
            
            private final ObjectNodeReader reader;
            
            public CompressedBucketIterator(ObjectNodeReader reader) {
                this.reader = Objects.requireNonNull(reader);
            }
            @Override
            protected ObjectNode computeNext() {
                ObjectNode next = reader.read();
                if(next == null) {
                    return endOfData();
                } else {                        
                    return next;
                }
            }
        }
    }
    
    public static class ExpandedBucket implements Iterable<ObjectNode> {
        
        private final ReadWriteLock lock;
        private final List<ObjectNode> nodes;
        
        public ExpandedBucket() {
            this(null);
        }
        public ExpandedBucket(Iterable<ObjectNode> nodes) {
            if(nodes instanceof CompressedBucket) {
                this.nodes = new ArrayList<ObjectNode>(((CompressedBucket) nodes).getNodeCount() + 16);
                for(ObjectNode node : nodes) {                    
                    this.nodes.add(node);
                }
            } else if(nodes == null) {
                this.nodes = new ArrayList<>();
            } else {
                this.nodes = Lists.newArrayList(nodes);                
            }
            this.lock = new ReentrantReadWriteLock();
        }
        public int getNodeCount() {
            this.lock.readLock().lock();
            try {
                return nodes.size();
            } finally {
                this.lock.readLock().unlock();
            }
        }
        @Override
        public Iterator<ObjectNode> iterator() {
            return new ExpandedBucketIterator();
        }
        public void add(ObjectNode node) {
            this.lock.writeLock().lock();
            try {
                this.nodes.add(node);
            } finally {
                this.lock.writeLock().unlock();
            }
        }
        public Optional<ObjectNode> get(int index) {
            this.lock.readLock().lock();
            try {
                if(index < 0 || index >= nodes.size()) {
                    return Optional.absent();
                } else {
                    return Optional.fromNullable(nodes.get(index));
                }
            } finally {
                this.lock.readLock().unlock();
            }
        }
        public class ExpandedBucketIterator extends AbstractIterator<ObjectNode> {
            
            private int index;
            
            public ExpandedBucketIterator() {
                this.index = 0;
            }
            @Override
            protected ObjectNode computeNext() {
                Optional<ObjectNode> optional = get(index);
                index++;
                if(optional.isPresent()) {
                    return optional.get();
                } else {
                    return endOfData();
                }
            }
        }
    }
}