package de.ck35.metricstore.cache.core.buckets;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

@ManagedResource
public class BucketManager {
    
    private static final Function<Entry<MetricBucket, ?>, MetricBucket> BUCKET_EXTRACT = new MapEntryKeyExtractFunction<MetricBucket>();
    
    private final Function<MetricBucket, ExpandedBucketManager> expandedBucketManagerFactory;
    private final Function<InputStream, ObjectNodeReader> objectNodeReaderFactory;
    private final Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory;
    private final ConcurrentMap<String, Entry<MetricBucket, CachedMetricBucket>> buckets;
    
    private final AtomicLong totalCreatedCachedMetricBuckets;
    
    public BucketManager(Function<MetricBucket, ExpandedBucketManager> expandedBucketManagerFactory,
                         Function<InputStream, ObjectNodeReader> objectNodeReaderFactory,
                         Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory) {
        this.expandedBucketManagerFactory = expandedBucketManagerFactory;
        this.objectNodeReaderFactory = objectNodeReaderFactory;
        this.objectNodeWriterFactory = objectNodeWriterFactory;
        this.buckets = new ConcurrentHashMap<>();
        this.totalCreatedCachedMetricBuckets = new AtomicLong();
    }

    public void write(StoredMetric metric) {
        Entry<MetricBucket, CachedMetricBucket> bucket = buckets.get(metric.getMetricBucket().getName());
        if(bucket == null) {
            Entry<MetricBucket, CachedMetricBucket> newBucket = metricBucketEntry(metric);
            Entry<MetricBucket, CachedMetricBucket> oldBucket = buckets.putIfAbsent(metric.getMetricBucket().getName(), newBucket);
            if(oldBucket == null) {
                totalCreatedCachedMetricBuckets.incrementAndGet();
                bucket = newBucket;
            } else {
                bucket = oldBucket;
            }
        }
        bucket.getValue().write(metric.getTimestamp(), metric.getObjectNode());
    }

    public Entry<Interval, Iterable<StoredMetric>> read(String bucketName, Interval interval) {
        Entry<MetricBucket, CachedMetricBucket> entry = buckets.get(bucketName);
        if(entry == null) {
            return emptyReadResult(interval);
        } else {
            NavigableMap<DateTime, MinuteBucket> subMap = entry.getValue().apply(interval);
            if(subMap.isEmpty()) {
                return emptyReadResult(interval);
            } else {
                return Maps.<Interval, Iterable<StoredMetric>>immutableEntry(new Interval(subMap.firstKey(), subMap.lastKey().plusMinutes(1)), new StoredMetricIterable(entry.getKey(), subMap));
            }
        }
    }
    
    /**
     * Remove all mappings which are before the given timestamp for all known {@link MetricBucket}. 
     * The timestamp is not inclusive.
     * 
     * @param before All mappings with a key which are before this timetamp will be removed.
     */
    public void clear(DateTime before) {
        for(Entry<MetricBucket, CachedMetricBucket> entry : buckets.values()) {
            entry.getValue().clear(before);
        }
    }
    
    /**
     * @return All known Buckets.
     */
    public Iterable<MetricBucket> listBuckets() {
        return Iterables.transform(buckets.values(), BUCKET_EXTRACT);
    }

    private Entry<Interval, Iterable<StoredMetric>> emptyReadResult(Interval interval) {
        return Maps.<Interval, Iterable<StoredMetric>>immutableEntry(new Interval(interval.getEnd(), interval.getEnd()), Collections.<StoredMetric>emptyList());
    }
    
    protected Entry<MetricBucket, CachedMetricBucket> metricBucketEntry(StoredMetric metric) {
        MinuteBucketSupplier bucketSupplier = new MinuteBucketSupplier(expandedBucketManagerFactory.apply(metric.getMetricBucket()), 
                                                                       objectNodeReaderFactory, 
                                                                       objectNodeWriterFactory);
        return Maps.immutableEntry(metric.getMetricBucket(), new CachedMetricBucket(bucketSupplier));
    }
    
    @ManagedAttribute
    public long getTotalCreatedCachedMetricBuckets() {
        return totalCreatedCachedMetricBuckets.get();
    }
    
    @ManagedOperation
    public Map<String, String> getDataIntervalPerBucket() {
        Map<String, String> result = new HashMap<>(buckets.size());
        for(Entry<String, Entry<MetricBucket, CachedMetricBucket>> entry : buckets.entrySet()) {
            Optional<Interval> dataInterval = entry.getValue().getValue().getDataInterval();
            if(dataInterval.isPresent()) {                
                result.put(entry.getKey(), dataInterval.get().toString());
            } else {
                result.put(entry.getKey(), "-");
            }
        }
        return result;
    }
    
    @ManagedOperation
    public Map<String, Long> getTotalObjectNodeCountPerBucket() {
        Map<String, Long> result = new HashMap<>(buckets.size());
        for(Entry<String, Entry<MetricBucket, CachedMetricBucket>> entry : buckets.entrySet()) {
            long size = 0;
            for(Entry<DateTime, MinuteBucket> minuteBucket : entry.getValue().getValue()) {
                size += minuteBucket.getValue().getSize();
            }
            result.put(entry.getKey(), size);
        }
        return result;
    }
    
    public static class StoredMetricIterable implements Iterable<StoredMetric> {

        private final MetricBucket bucket;
        private final NavigableMap<DateTime, MinuteBucket> subMap;

        public StoredMetricIterable(MetricBucket bucket,
                                    NavigableMap<DateTime, MinuteBucket> subMap) {
            this.bucket = bucket;
            this.subMap = subMap;
        }
        @Override
        public Iterator<StoredMetric> iterator() {
            return new StoredMetricIterator(bucket, new ReadIterator(subMap.entrySet().iterator()));
        }
    }
    
    public static class StoredMetricIterator extends AbstractIterator<StoredMetric> {
        
        private final MetricBucket bucket;
        private final Iterator<Entry<DateTime, ObjectNode>> objectNodesIter;
        
        public StoredMetricIterator(MetricBucket bucket,
                                    Iterator<Entry<DateTime, ObjectNode>> objectNodesIter) {
            this.bucket = bucket;
            this.objectNodesIter = objectNodesIter;
        }
        @Override
        protected StoredMetric computeNext() {
            if(objectNodesIter.hasNext()) {
                Entry<DateTime, ObjectNode> entry = objectNodesIter.next();
                return new ImmutableStoredMetric(bucket, entry.getKey(), entry.getValue());
            } else {
                return endOfData();
            }
        }
    }
    
    public static class ReadIterator extends AbstractIterator<Entry<DateTime, ObjectNode>> {
        
        private final Iterator<Entry<DateTime, MinuteBucket>> minuteBucketIterator;
        private DateTime currentTimestamp;
        private Iterator<ObjectNode> nodeIterator;
        
        public ReadIterator(Iterator<Entry<DateTime, MinuteBucket>> minuteBucketIterator) {
            this.minuteBucketIterator = minuteBucketIterator;
            this.nodeIterator = ImmutableSet.<ObjectNode>of().iterator();
        }

        @Override
        protected Entry<DateTime, ObjectNode> computeNext() {
            while(!nodeIterator.hasNext()) {
                if(!minuteBucketIterator.hasNext()) {
                    return endOfData();
                }
                Entry<DateTime, MinuteBucket> next = minuteBucketIterator.next();
                currentTimestamp = next.getKey();
                nodeIterator = next.getValue().iterator();
            }
            return Maps.immutableEntry(currentTimestamp, nodeIterator.next());
        }
    }
    
    public static class MapEntryKeyExtractFunction<K> implements Function<Entry<K, ?>, K> {
        @Override
        public K apply(Entry<K, ?> input) {
            return input == null ? null : input.getKey();
        }
    }
}