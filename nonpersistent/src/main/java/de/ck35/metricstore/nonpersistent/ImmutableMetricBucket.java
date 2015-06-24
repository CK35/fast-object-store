package de.ck35.metricstore.nonpersistent;

import java.util.Objects;

import de.ck35.metricstore.api.MetricBucket;

/**
 * Metric bucket implementation which is immutable. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class ImmutableMetricBucket implements MetricBucket {
    
    private final String name;
    private final String type;
    
    public ImmutableMetricBucket(String name, String type) {
        this.name = Objects.requireNonNull(name);
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }
    @Override
    public String getType() {
        return type;
    }
}