package de.ck35.metricstore.cache;

import java.util.regex.PatternSyntaxException;

import org.joda.time.Interval;

import de.ck35.metricstore.StoredMetricCallable;

/**
 * Representation of a cache read request. You can add new {@link StoredMetricCallable} at any time.
 * A callable will receive metric nodes after:
 * <ul>
 * <li>{@link MetricCacheRequest#build(StoredMetricCallable)}</li>
 * <li>{@link FieldFilterBuilder#build()}</li>
 * <li>{@link ValueFilterBuilder#build()}</li>
 * </ul>
 * {@link MetricCacheRequest#read(String, Interval)} must be called before any metric nodes are read
 * from the cache.
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public interface MetricCacheRequest {

    ReadFilter build(StoredMetricCallable callable);
    
    FieldFilterBuilder builder(StoredMetricCallable callable);
    
    Iterable<ReadFilter> getFilters();
    
    void read(String bucketName, Interval interval);
    
    public interface ReadFilter {
        
        StoredMetricCallable getCallable();
        
    }
    
    public interface FieldFilterBuilder {
        
        FieldFilterBuilder andFieldIsPresent(String fieldName);
        
        FieldFilterBuilder andFieldIsNotPresent(String fieldName);
        
        ValueFilterBuilder andValueMatches(String fieldName, String regex) throws PatternSyntaxException;
        
        ReadFilter build();
        
    }
    
    public interface ValueFilterBuilder {
        
        ValueFilterBuilder andValueMatches(String fieldName, String regex) throws PatternSyntaxException;
        
        ReadFilter build();
        
    }
}