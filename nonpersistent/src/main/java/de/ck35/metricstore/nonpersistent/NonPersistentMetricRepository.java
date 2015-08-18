package de.ck35.metricstore.nonpersistent;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;

/**
 * An implementation of the {@link MetricRepository} Interface which does not store metric data.
 * Only the bucket information is kept in memory for {@link #listBuckets()}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class NonPersistentMetricRepository implements MetricRepository {
    
    private final ConcurrentMap<String, MetricBucket> buckets;
    private final Function<ObjectNode, DateTime> timestampFunction;
    
    public NonPersistentMetricRepository(Function<ObjectNode, DateTime> timestampFunction) {
        this.timestampFunction = timestampFunction;
        this.buckets = new ConcurrentHashMap<>();
    }
    
    @Override
    public Iterable<MetricBucket> listBuckets() {
        return ImmutableList.copyOf(buckets.values());
    }

    @Override
    public StoredMetric write(String bucketName, String bucketType, ObjectNode node) {
        MetricBucket metricBucket = buckets.get(bucketName);
        if(metricBucket == null) {
            MetricBucket newBucket = new ImmutableMetricBucket(bucketName, bucketType);
            MetricBucket oldBucket = buckets.putIfAbsent(bucketName, newBucket);
            if(oldBucket == null) {
                metricBucket = newBucket;
            } else {
                metricBucket = oldBucket;
            }
        }
        DateTime timestamp = Objects.requireNonNull(timestampFunction.apply(node));
        return new ImmutableStoredMetric(metricBucket, timestamp, node);
    }
    
    @Override
    public void read(String bucketName, Interval interval, StoredMetricCallable callable) {
        //nothing to do here 
    }
}