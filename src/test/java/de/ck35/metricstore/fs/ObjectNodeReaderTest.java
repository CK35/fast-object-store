package de.ck35.metricstore.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.metricstore.fs.ObjectNodeReader;
import de.ck35.metricstore.fs.configuration.ObjectMapperConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={ObjectMapperConfiguration.class})
public class ObjectNodeReaderTest {

	private static final Logger LOG = LoggerFactory.getLogger(ObjectNodeReaderTest.class);
	
	@Autowired ResourceLoader resourceLoader;
	@Autowired ObjectMapper mapper;
	
	@Test
	public void testRead() throws IOException {
		Path tempFile = Files.createTempFile("objectReaderTest", ".json");
		LOG.debug("Working on tempfile: '{}'.", tempFile);
		Resource resource = resourceLoader.getResource("classpath:read-test-files/read-test.json");
		assertTrue(resource.toString() + " is not readable!", resource.isReadable());
		GzipUtils.gzip(Paths.get(resource.getURI()), tempFile);
		assertTrue(Files.size(tempFile) > 0);
		try(ObjectNodeReader reader = new ObjectNodeReader(tempFile, mapper)) {
			ObjectNode node1 = reader.read();
			ObjectNode node2 = reader.read();
			ObjectNode node3 = reader.read();
			assertNotNull(node1);
			assertNotNull(node2);
			assertNotNull(node3);
			assertNull(reader.read());
			assertEquals(4, reader.getIgnoredObjects());
		}
		Files.delete(tempFile);
	}

}