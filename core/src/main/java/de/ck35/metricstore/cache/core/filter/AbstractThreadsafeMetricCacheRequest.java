package de.ck35.metricstore.cache.core.filter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.MetricCacheRequest;
import de.ck35.metricstore.util.JsonNodeExtractor;

public abstract class AbstractThreadsafeMetricCacheRequest implements MetricCacheRequest, 
                                                                      Function<String, Function<ObjectNode, JsonNode>> {

    protected final List<ImmutableReadFilter> filters;
    private final LoadingCache<String, Function<ObjectNode, JsonNode>> functionCache;
    
    public AbstractThreadsafeMetricCacheRequest() {
        this.filters = new CopyOnWriteArrayList<>();
        this.functionCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Function<ObjectNode, JsonNode>>() {
            @Override
            public Function<ObjectNode, JsonNode> load(String key) {
                return JsonNodeExtractor.forPath(key);
            }
        });
    }
    @Override
    public ReadFilter build(StoredMetricCallable callable) {
        return builder(callable).build();
    }
    @Override
    public FieldFilterBuilder builder(StoredMetricCallable callable) {
        return new FilterBuilder(callable, filters, this);
    }
    @Override
    public Iterable<ReadFilter> getFilters() {
        return Collections.<ReadFilter>unmodifiableList(filters);
    }
    @Override
    public Function<ObjectNode, JsonNode> apply(String input) {
        try {
            return functionCache.get(input);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}