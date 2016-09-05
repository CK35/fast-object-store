package de.ck35.metricstore.cache.core;

import org.joda.time.Interval;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;

import de.ck35.metricstore.StoredMetric;

public class CacheablePredicate implements Predicate<StoredMetric> {
    
    private final Supplier<Interval> cacheIntervalSupplier;
    
    public CacheablePredicate(Supplier<Interval> cacheIntervalSupplier) {
        this.cacheIntervalSupplier = cacheIntervalSupplier;
    }

    @Override
    public boolean apply(StoredMetric input) {
        if(input == null) {
            return false;
        }
        return !input.getTimestamp().isBefore(cacheIntervalSupplier.get().getStart());
    }

}