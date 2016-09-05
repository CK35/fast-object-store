package de.ck35.metricstore.cache.core;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.google.common.base.Supplier;

@ManagedResource
public class UTCCacheIntervalSupplier implements Supplier<Interval> {

    private final Period cachePeriod;
    private final Supplier<DateTime> clock;
    
    public UTCCacheIntervalSupplier(Period cachePeriod, Supplier<DateTime> clock) {
        this.cachePeriod = cachePeriod;
        this.clock = clock;
    }

    @Override
    public Interval get() {
        return getCurrentCacheInterval();
    }
    
    /**
     * @return The interval of cacheable data.
     */
    public Interval getCurrentCacheInterval() {
        return new Interval(cachePeriod, clock.get().withZone(DateTimeZone.UTC));
    }
    
    @ManagedAttribute
    public String getCurrentCacheIntervalISO() {
        return getCurrentCacheInterval().toString();
    }
    
    public static class Clock implements Supplier<DateTime> {
        @Override
        public DateTime get() {
            return DateTime.now();
        }
    }
}