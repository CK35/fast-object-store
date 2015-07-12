package de.ck35.metricstore.util.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.metricstore.util.configuration.ObjectMapperConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={ObjectMapperConfiguration.class})
public class ObjectNodeReadWriterTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(ObjectNodeReadWriterTest.class);
	
	@Autowired ObjectMapper mapper;
	
	@Test
	public void testReadWrite() throws IOException {
		JsonNodeFactory nodeFactory = mapper.getNodeFactory();
		ObjectNode node1 = nodeFactory.objectNode();
		node1.set("field1", nodeFactory.textNode("value1"));
		ArrayNode arrayNode = nodeFactory.arrayNode();
		arrayNode.add(1);
		arrayNode.add(2);
		arrayNode.add(3);
		node1.set("field2", arrayNode);
		node1.set("field3", nodeFactory.booleanNode(false));
		
		ObjectNode node2 = nodeFactory.objectNode();
		node2.set("field2", nodeFactory.textNode("value2"));
		
		ObjectNode node3 = nodeFactory.objectNode();
		node3.set("field3", nodeFactory.textNode("value3"));
		
		Path file = Files.createTempFile("objectWriteTest", ".json");
		LOG.debug("Working on temp file: '{}'.", file);
		try (ObjectNodeWriter writer = new ObjectNodeWriter(file, mapper.getFactory())) {
			writer.write(node1);
		}
		try (ObjectNodeWriter writer = new ObjectNodeWriter(file, mapper.getFactory())) {
            writer.write(node2);
            writer.write(node3);
        }
		assertTrue(Files.size(file) > 0);
		try (ObjectNodeReader reader = new ObjectNodeReader(file, mapper)) {
			ObjectNode node1r = reader.read();
			ObjectNode node2r = reader.read();
			ObjectNode node3r = reader.read();
			assertEquals(node1, node1r);
			assertEquals(node2, node2r);
			assertEquals(node3, node3r);
			assertNull(reader.read());
		}
		Files.delete(file);
	}
}