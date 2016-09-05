package de.ck35.metricstore.cache.core.filter;

import java.util.Map.Entry;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.MetricCacheRequest.ReadFilter;

public class ImmutableReadFilter implements ReadFilter {
    
    private final StoredMetricCallable callable;
    private final Iterable<Function<ObjectNode, JsonNode>> requiredPresentFields;
    private final Iterable<Function<ObjectNode, JsonNode>> requiredNonPresentFields;
    private final Iterable<Entry<Function<ObjectNode, JsonNode>, Pattern>> valueFields;
    
    public ImmutableReadFilter(StoredMetricCallable callable,
                               Iterable<Function<ObjectNode, JsonNode>> requiredPresentFields,
                               Iterable<Function<ObjectNode, JsonNode>> requiredNonPresentFields,
                               Iterable<Entry<Function<ObjectNode, JsonNode>, Pattern>> valueFields) {
        this.callable = callable;
        this.requiredPresentFields = ImmutableList.copyOf(requiredPresentFields);
        this.requiredNonPresentFields = ImmutableList.copyOf(requiredNonPresentFields);
        this.valueFields = ImmutableList.copyOf(valueFields);
    }
    @Override
    public StoredMetricCallable getCallable() {
        return callable;
    }
    public Iterable<Function<ObjectNode, JsonNode>> getRequiredPresentFields() {
        return requiredPresentFields;
    }
    public Iterable<Function<ObjectNode, JsonNode>> getRequiredNonPresentFields() {
        return requiredNonPresentFields;
    }
    public Iterable<Entry<Function<ObjectNode, JsonNode>, Pattern>> getValueFields() {
        return valueFields;
    }
}