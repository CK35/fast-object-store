package de.ck35.metricstore.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

public class JsonNodeExtractorTest {

	private ObjectNode testNode;
	
	public JsonNodeExtractorTest() {
		JsonNodeFactory nodeFactory = new ObjectMapper().getNodeFactory();
		ObjectNode root = nodeFactory.objectNode();
		root.put("key1", "value1");
		
		ObjectNode a = nodeFactory.objectNode();
		a.put("key2", "value2");
		root.put("a", a);
		
		ObjectNode b = nodeFactory.objectNode();
		b.put("key3", "value3");
		a.put("b", b);

		ObjectNode c = nodeFactory.objectNode();
		c.put("key4", "value4");
		b.put("c", c);
		this.testNode = root;
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testForPathWithNull() {
		JsonNodeExtractor.forPath(null);
	}
	@Test(expected=IllegalArgumentException.class)
	public void testForPathWithEmpty() {
		JsonNodeExtractor.forPath(" ");
	}
	@Test(expected=IllegalArgumentException.class)
	public void testForPathWithEmptyList() {
		JsonNodeExtractor.forPath(" . ");
	}
	
	@Test
	public void testForOneTokenPath() {
		Function<ObjectNode, JsonNode> function = JsonNodeExtractor.forPath("key1");
		assertNull(function.apply(null));
		assertEquals("value1", function.apply(testNode).asText());
	}
	@Test
	public void testForTwoTokensPath() {
		Function<ObjectNode, JsonNode> function = JsonNodeExtractor.forPath("a.key2");
		assertNull(function.apply(null));
		assertEquals("value2", function.apply(testNode).asText());
	}
	@Test
	public void testForThreeTokensPath() {
		Function<ObjectNode, JsonNode> function = JsonNodeExtractor.forPath("a.b.key3");
		assertNull(function.apply(null));
		assertEquals("value3", function.apply(testNode).asText());
	}
	@Test
	public void testForNTokensPath() {
		Function<ObjectNode, JsonNode> function = JsonNodeExtractor.forPath("a.b.c.key4");
		assertNull(function.apply(null));
		assertEquals("value4", function.apply(testNode).asText());
	}
}