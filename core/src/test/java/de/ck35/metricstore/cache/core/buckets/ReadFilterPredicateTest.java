package de.ck35.metricstore.cache.core.buckets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.core.buckets.ReadFilterPredicate;
import de.ck35.metricstore.cache.core.filter.ImmutableReadFilter;

@RunWith(MockitoJUnitRunner.class)
public class ReadFilterPredicateTest {

    private ObjectNode objectNode;
    private JsonNodeFactory nodeFactory;
    
    @Mock StoredMetricCallable callable;
    @Mock Function<ObjectNode, JsonNode> requiredPresentField;
    @Mock Function<ObjectNode, JsonNode> requiredNonPresentField;
    @Mock Function<ObjectNode, JsonNode> valueField;
    
    @Before
    public void before() {
        nodeFactory = new ObjectMapper().getNodeFactory();
        objectNode = nodeFactory.objectNode();
    }
    
    public ReadFilterPredicate readFilterPredicate() {
        ReadFilterPredicate predicate = new ReadFilterPredicate(objectNode);
        return predicate;
    }
    
    public ImmutableReadFilter readFilter(Function<ObjectNode, JsonNode> requiredPresentField,
                                          Function<ObjectNode, JsonNode> requiredNonPresentField,
                                          Function<ObjectNode, JsonNode> valueField, 
                                          Pattern pattern) {
        return new ImmutableReadFilter(callable, 
                                       requiredPresentField == null ? Collections.<Function<ObjectNode, JsonNode>>emptySet() : Collections.singleton(requiredPresentField), 
                                       requiredNonPresentField == null ? Collections.<Function<ObjectNode, JsonNode>>emptySet() : Collections.singleton(requiredNonPresentField), 
                                       Collections.singleton(Maps.immutableEntry(valueField, pattern)));
    }
    
    @Test
    public void testApplyExtractFunctionIsOnlyCalledOnce() {
        Pattern pattern = Pattern.compile("a");
        JsonNode node = nodeFactory.textNode("a");
        when(requiredPresentField.apply(objectNode)).thenReturn(node);
        
        ImmutableReadFilter readFilter = readFilter(requiredPresentField, null, requiredPresentField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertTrue(predicate.apply(readFilter));
        verify(requiredPresentField).apply(objectNode);
    }
    
    @Test
    public void testApplyFALSEWithMissingNode() {
        Pattern pattern = Pattern.compile("a");
        when(requiredPresentField.apply(objectNode)).thenReturn(MissingNode.getInstance());
        when(valueField.apply(objectNode)).thenReturn(nodeFactory.textNode("a"));
        
        ImmutableReadFilter readFilter = readFilter(requiredPresentField, null, valueField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertFalse(predicate.apply(readFilter));
    }
    
    @Test
    public void testApplyFALSEWithMissingNodeNotPresent() {
        Pattern pattern = Pattern.compile("a");
        when(requiredNonPresentField.apply(objectNode)).thenReturn(nodeFactory.textNode("b"));
        when(valueField.apply(objectNode)).thenReturn(nodeFactory.textNode("a"));
        
        ImmutableReadFilter readFilter = readFilter(null, requiredNonPresentField, valueField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertFalse(predicate.apply(readFilter));
    }
    
    @Test
    public void testApplyTRUEWithMissingNodePresent() {
        Pattern pattern = Pattern.compile("a");
        when(requiredNonPresentField.apply(objectNode)).thenReturn(MissingNode.getInstance());
        when(valueField.apply(objectNode)).thenReturn(nodeFactory.textNode("a"));
        
        ImmutableReadFilter readFilter = readFilter(null, requiredNonPresentField, valueField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertTrue(predicate.apply(readFilter));
    }
    
    @Test
    public void testApplyNullNode() {
        Pattern pattern = Pattern.compile("a");
        JsonNode node = nodeFactory.nullNode();
        when(valueField.apply(objectNode)).thenReturn(node);
        
        ImmutableReadFilter readFilter = readFilter(null, null, valueField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertFalse(predicate.apply(readFilter));
    }
    
    @Test
    public void testApplyMissingNode() {
        Pattern pattern = Pattern.compile("a");
        JsonNode node = MissingNode.getInstance();
        when(valueField.apply(objectNode)).thenReturn(node);
        
        ImmutableReadFilter readFilter = readFilter(null, null, valueField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertFalse(predicate.apply(readFilter));
    }
    
    @Test
    public void testApplyBooleanNode() {
        Pattern pattern = Pattern.compile("true");
        JsonNode node = nodeFactory.booleanNode(true);
        when(requiredPresentField.apply(objectNode)).thenReturn(node);
        
        ImmutableReadFilter readFilter = readFilter(requiredPresentField, null, requiredPresentField, pattern);
        ReadFilterPredicate predicate = readFilterPredicate();
        assertTrue(predicate.apply(readFilter));
    }
    
}