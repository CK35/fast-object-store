package de.ck35.metricstore.cache.core.buckets;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import de.ck35.metricstore.cache.core.filter.ImmutableReadFilter;

public class ReadFilterPredicate implements Predicate<ImmutableReadFilter> {

    private final ObjectNode objectNode;
    private final LoadingCache<Function<ObjectNode,JsonNode>,JsonNode> cache;
    
    public ReadFilterPredicate(ObjectNode objectNode) {
        this.objectNode = Objects.requireNonNull(objectNode);
        this.cache = CacheBuilder.newBuilder().build(new CacheLoader<Function<ObjectNode, JsonNode>, JsonNode>() {
            @Override
            public JsonNode load(Function<ObjectNode, JsonNode> key) {
                return key.apply(ReadFilterPredicate.this.objectNode);
            }
        });
    }

    @Override
    public boolean apply(ImmutableReadFilter input) {
        for(Function<ObjectNode, JsonNode> presentField : input.getRequiredPresentFields()) {
            if(get(presentField).isMissingNode()) {
                return false;
            }
        }
        for(Function<ObjectNode, JsonNode> misingField : input.getRequiredNonPresentFields()) {
            if(!get(misingField).isMissingNode()) {
                return false;
            }
        }
        for(Entry<Function<ObjectNode, JsonNode>, Pattern> pattern : input.getValueFields()) {
            if(!pattern.getValue().matcher(get(pattern.getKey()).asText()).matches()) {
                return false;
            }
        }
        return true;
    }
    
    protected JsonNode get(Function<ObjectNode, JsonNode> function) {
        try {
            return cache.get(function);
        } catch (ExecutionException e) {
            throw new RuntimeException("Could not load json node from object node!", e);
        }
    }
    
}