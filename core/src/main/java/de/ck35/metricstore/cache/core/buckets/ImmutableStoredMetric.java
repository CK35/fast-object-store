package de.ck35.metricstore.cache.core.buckets;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.StoredMetric;

/**
 * Stored metric implementation which is immutable.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class ImmutableStoredMetric implements StoredMetric {

    private final MetricBucket metricBucket;
    private final DateTime timestamp;
    private final ObjectNode objectNode;
    
    public ImmutableStoredMetric(MetricBucket metricBucket,
                                 DateTime timestamp,
                                 ObjectNode objectNode) {
        this.metricBucket = metricBucket;
        this.timestamp = timestamp;
        this.objectNode = objectNode;
    }
    
    @Override
    public MetricBucket getMetricBucket() {
        return metricBucket;
    }
    @Override
    public DateTime getTimestamp() {
        return timestamp;
    }
    @Override
    public ObjectNode getObjectNode() {
        return objectNode;
    }

}