package de.ck35.metricstore.cache.core.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.MetricCacheRequest.FieldFilterBuilder;
import de.ck35.metricstore.cache.MetricCacheRequest.ReadFilter;
import de.ck35.metricstore.cache.MetricCacheRequest.ValueFilterBuilder;

public class FilterBuilder implements FieldFilterBuilder, ValueFilterBuilder {
    
    private final List<Function<ObjectNode, JsonNode>> requiredPresentFields;
    private final List<Function<ObjectNode, JsonNode>> requiredNonPresentFields;
    private final List<Entry<Function<ObjectNode, JsonNode>, Pattern>> valueFields;
    
    private final StoredMetricCallable callable;
    private final Collection<ImmutableReadFilter> filters;
    private final Function<String, Function<ObjectNode, JsonNode>> extractFunctions;
    
    public FilterBuilder(StoredMetricCallable callable, 
                         Collection<ImmutableReadFilter> filters,
                         Function<String, Function<ObjectNode, JsonNode>> extractFunctions) {
        this.callable = callable;
        this.filters = filters;
        this.extractFunctions = extractFunctions;
        this.requiredPresentFields = new ArrayList<>();
        this.requiredNonPresentFields = new ArrayList<>();
        this.valueFields = new ArrayList<>();
    }
    
    @Override
    public FieldFilterBuilder andFieldIsPresent(String fieldName) {
        this.requiredPresentFields.add(extractFunctions.apply(fieldName));
        return this;
    }
    @Override
    public FieldFilterBuilder andFieldIsNotPresent(String fieldName) {
        this.requiredNonPresentFields.add(extractFunctions.apply(fieldName));
        return this;
    }
    @Override
    public ValueFilterBuilder andValueMatches(String fieldName, String regex) throws PatternSyntaxException {
        this.valueFields.add(Maps.immutableEntry(extractFunctions.apply(fieldName), Pattern.compile(regex)));
        return this;
    }
    @Override
    public ReadFilter build() {
        ImmutableReadFilter filter = new ImmutableReadFilter(callable, 
                                                             requiredPresentFields, 
                                                             requiredNonPresentFields, 
                                                             valueFields);
        filters.add(filter);
        return filter;
    }
}