package de.ck35.metricstore.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.util.TimestampFunction;
import de.ck35.metricstore.util.io.ObjectNodeReader;

public class StoredObjectNodeReaderTest {
	
	private MetricBucket bucket;
	private ObjectNodeReader reader;
	private Function<ObjectNode, DateTime> timestampFunction;
	
	public StoredObjectNodeReaderTest() {
		bucket = mock(MetricBucket.class);
		reader = mock(ObjectNodeReader.class);
		timestampFunction = new TimestampFunction();
	}
	
	public StoredObjectNodeReader storedObjectNodeReader() {
		return new StoredObjectNodeReader(bucket, reader, timestampFunction);
	}
	
	@Test
	public void testReaderReturnsNull() {
		StoredObjectNodeReader reader = storedObjectNodeReader();
		assertNull(reader.read());
	}
	
	@Test
	public void testReaderNoTimestamp() throws IOException {
		JsonNodeFactory factory = new ObjectMapper().getNodeFactory();
		ObjectNode node1 = factory.objectNode();
		ObjectNode node2 = factory.objectNode();
		node2.put(TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME, new LocalDateTime(2015,1,1,0,0).toString());
		final Iterator<ObjectNode> nodes = ImmutableList.of(node1, node2).iterator();
		when(reader.read()).then(new Answer<ObjectNode>() {
			@Override
			public ObjectNode answer(InvocationOnMock invocation) {
				if(nodes.hasNext()) {					
					return nodes.next();
				} else {
					return null;
				}
			}
		});
		when(reader.getIgnoredObjectsCount()).thenReturn(5);
		try(StoredObjectNodeReader reader = storedObjectNodeReader()) {			
			StoredMetric metric = reader.read();
			assertNotNull(metric);
			assertNull(reader.read());
			assertEquals(6, reader.getIgnoredObjectsCount());
			assertEquals(bucket, metric.getMetricBucket());
			assertEquals(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), metric.getTimestamp());
			assertEquals(node2, metric.getObjectNode());
		}
	}
	

}