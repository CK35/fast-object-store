package de.ck35.objectstore.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

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

import de.ck35.objectstore.fs.configuration.ObjectMapperConfiguration;

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
		try(InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(resource.getURI())));
			OutputStream out = new BufferedOutputStream(Files.newOutputStream(tempFile));
			GZIPOutputStream gzOut = new GZIPOutputStream(out)) {
			for(int next = in.read() ; next != -1 ; next = in.read()) {
				gzOut.write(next);
			}
		}
		assertTrue(Files.size(tempFile) > 0);
		try(ObjectNodeReader reader = new ObjectNodeReader(tempFile, mapper)) {
			assertNotNull(reader.read());
			assertNotNull(reader.read());
			assertNotNull(reader.read());
			assertNull(reader.read());
			assertEquals(4, reader.getIgnoredObjects());
		}
		Files.delete(tempFile);
	}

}