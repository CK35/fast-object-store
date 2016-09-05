package de.ck35.metricstore.cache.core.buckets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.cache.core.buckets.BucketExpandListener;
import de.ck35.metricstore.cache.core.buckets.MinuteBucket;
import de.ck35.metricstore.configuration.ObjectMapperConfiguration;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

@RunWith(MockitoJUnitRunner.class)
public class MinuteBucketTest {

    @Mock BucketExpandListener expandListener;

    private ObjectMapper mapper;
    private Function<InputStream, ObjectNodeReader> objectNodeReaderFactory;
    private Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory;
    
    public MinuteBucketTest() {
        this.mapper = ObjectMapperConfiguration.objectMapper();
        this.objectNodeReaderFactory = new ObjectNodeReader.StreamFactory(mapper, Charsets.UTF_8);
        this.objectNodeWriterFactory = new ObjectNodeWriter.StreamFactory(mapper.getFactory(), Charsets.UTF_8);
    }
    
    public MinuteBucket minuteBucket() {
        return new MinuteBucket(objectNodeReaderFactory, objectNodeWriterFactory, expandListener);
    }
    
    @Test
    public void testInitialState() {
        MinuteBucket minuteBucket = minuteBucket();
        assertTrue(minuteBucket.isCompressed());
        assertFalse(minuteBucket.iterator().hasNext());
        assertEquals(0, minuteBucket.getSize());
    }
    
    @Test
    public void testWriteReadUncompressed() {
        ObjectNode node1 = mapper.getNodeFactory().objectNode();
        ObjectNode node2 = mapper.getNodeFactory().objectNode();
        node1.put("a", "a1");
        node2.put("b", "b1");
        
        MinuteBucket minuteBucket = minuteBucket();
        minuteBucket.write(node1);
        minuteBucket.write(node2);
        
        verify(expandListener).expanded(minuteBucket);
        assertFalse(minuteBucket.isCompressed());
        assertEquals(2, minuteBucket.getSize());
        assertEquals(ImmutableList.of(node1, node2),  ImmutableList.copyOf(minuteBucket.iterator()));
    }
    
    @Test
    public void testWriteReadCompressed() {
        ObjectNode node1 = mapper.getNodeFactory().objectNode();
        ObjectNode node2 = mapper.getNodeFactory().objectNode();
        node1.put("a", "a1");
        node2.put("b", "b1");
        
        MinuteBucket minuteBucket = minuteBucket();
        minuteBucket.write(node1);
        minuteBucket.write(node2);
        minuteBucket.compress();
        
        verify(expandListener).expanded(minuteBucket);
        assertTrue(minuteBucket.isCompressed());
        assertEquals(2, minuteBucket.getSize());
        assertEquals(ImmutableList.of(node1, node2),  ImmutableList.copyOf(minuteBucket.iterator()));
    }
    
    @Test
    public void testWriteCompressed() {
        ObjectNode node1 = mapper.getNodeFactory().objectNode();
        ObjectNode node2 = mapper.getNodeFactory().objectNode();
        node1.put("a", "a1");
        node2.put("b", "b1");
        
        MinuteBucket minuteBucket = minuteBucket();
        minuteBucket.write(node1);
        minuteBucket.compress();
        minuteBucket.write(node2);
        
        verify(expandListener, times(2)).expanded(minuteBucket);
        assertFalse(minuteBucket.isCompressed());
        assertEquals(2, minuteBucket.getSize());
        assertEquals(ImmutableList.of(node1, node2),  ImmutableList.copyOf(minuteBucket.iterator()));
    }

}