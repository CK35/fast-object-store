package de.ck35.metricstore.cache.core.buckets;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;

public class CachedMetricBucket implements Iterable<Entry<DateTime, MinuteBucket>> , Function<Interval, NavigableMap<DateTime, MinuteBucket>> {

    private final ConcurrentNavigableMap<DateTime, MinuteBucket> minuteBuckets;
    private final Supplier<MinuteBucket> minuteBucketSupplier;
    
    public CachedMetricBucket(Supplier<MinuteBucket> minuteBucketSupplier) {
        this.minuteBucketSupplier = minuteBucketSupplier;
        this.minuteBuckets = new ConcurrentSkipListMap<>();
    }
    
    public void write(DateTime timestamp, ObjectNode objectNode) {
        DateTime utcMinuteTimestamp = timestamp.withSecondOfMinute(0).withMillisOfSecond(0).withZone(DateTimeZone.UTC);
        MinuteBucket bucket = minuteBuckets.get(utcMinuteTimestamp);
        if(bucket == null) {
            MinuteBucket newBucket = minuteBucketSupplier.get();
            MinuteBucket oldBucket = minuteBuckets.putIfAbsent(utcMinuteTimestamp, newBucket);
            if(oldBucket == null) {
                bucket = newBucket;
            } else {
                bucket = oldBucket;
            }
        }
        bucket.write(objectNode);
    }
    
    @Override
    public NavigableMap<DateTime, MinuteBucket> apply(Interval input) {
        return input == null ? null : new TreeMap<>(minuteBuckets.subMap(input.getStart(), input.getEnd()));
    }
    
    /**
     * Remove all mappings which are before the given timestamp. The timestamp is not inclusive.
     * 
     * @param before All mappings with a key which are before this timetamp will be removed.
     */
    public void clear(DateTime before) {
        minuteBuckets.headMap(before).clear();
    }
    
    @Override
    public Iterator<Entry<DateTime, MinuteBucket>> iterator() {
        return minuteBuckets.entrySet().iterator();
    }
    
    public Optional<Interval> getDataInterval() {
        try {            
            return Optional.of(new Interval(minuteBuckets.firstKey(), minuteBuckets.lastKey().plusMinutes(1)));
        } catch(NoSuchElementException e) {
            return Optional.absent();
        }
    }
}