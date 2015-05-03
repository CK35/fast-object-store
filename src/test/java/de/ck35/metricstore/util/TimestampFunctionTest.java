package de.ck35.metricstore.util;

import static de.ck35.metricstore.util.TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME;
import static org.junit.Assert.*;

import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import de.ck35.metricstore.util.TimestampFunction;

public class TimestampFunctionTest {

	@Test
	public void testApplyNull() {
		TimestampFunction function = new TimestampFunction();
		assertNull(function.apply(null));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testApplyMissingTimestampField() {
		ObjectNode node = map(ImmutableMap.<String, Object>of("a", "a1"));
		TimestampFunction function = new TimestampFunction();
		function.apply(node);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testApplyEmptyTimestampField() {
		ObjectNode node = map(ImmutableMap.<String, Object>of(DEFAULT_TIMESTAMP_FILED_NAME, ""));
		TimestampFunction function = new TimestampFunction();
		function.apply(node);
	}
	
	@Test
	public void testApplyTimestampWithoutZone() {
		String timestamp = new LocalDateTime(2015, 1, 1, 0, 0).toString();
		ObjectNode node = map(ImmutableMap.<String, Object>of(DEFAULT_TIMESTAMP_FILED_NAME, timestamp));
		TimestampFunction function = new TimestampFunction();
		DateTime result = function.apply(node);
		assertEquals(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), result);
	}
	
	@Test
	public void testApplyTimestampWitZone() {
		DateTimeZone zone = DateTimeZone.forID("Europe/Berlin");
		String timestamp = new DateTime(2015, 1, 1, 1, 0, zone).toString();
		ObjectNode node = map(ImmutableMap.<String, Object>of(DEFAULT_TIMESTAMP_FILED_NAME, timestamp));
		TimestampFunction function = new TimestampFunction();
		DateTime result = function.apply(node);
		assertEquals(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), result);
	}

	public static ObjectNode map(Map<String, Object> map) {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.convertValue(map, ObjectNode.class);
	}
}