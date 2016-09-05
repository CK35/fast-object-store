package de.ck35.metricstore.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketDataTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(BucketDataTest.class);
	private Path workdir;
	
	@Before
	public void before() throws IOException {
		workdir = Files.createTempDirectory("BucketDataTest");
		LOG.debug("Running test inside tmp work dir: '{}'.", workdir);
	}
	
	@After
	public void after() throws IOException {
		WritableFilesystemBucket.clearDirectory(workdir);
		Files.delete(workdir);
	}

	@Test
	public void testCreate() throws IOException {
		String name = "my-test-bucket";
		String type = "my-test-type";
		BucketData bucketData = BucketData.create(workdir, name, type);
		assertEquals(name, bucketData.getName());
		assertEquals(type, bucketData.getType());
		assertEquals(workdir.resolve(name), bucketData.getBasePath());
		assertTrue(bucketData.toString().contains(name));
		
		BucketData load = BucketData.load(workdir.resolve("my-test-bucket"));
		assertEquals(name, load.getName());
		assertEquals(type, load.getType());
	}
	
	@Test
	public void testCreateWithoutType() throws IOException {
		String name = "my-test-bucket";
		String type = null;
		BucketData bucketData = BucketData.create(workdir, name, type);
		assertEquals(name, bucketData.getName());
		assertEquals(type, bucketData.getType());
		assertEquals(workdir.resolve(name), bucketData.getBasePath());
		assertTrue(bucketData.toString().contains(name));
		
		BucketData load = BucketData.load(workdir.resolve("my-test-bucket"));
		assertEquals(name, load.getName());
		assertEquals(type, load.getType());
	}
	
	@Test(expected=IOException.class)
	public void testLoadFails() throws IOException {
		Path file = Files.createFile(workdir.resolve("my-test-bucket"));
		BucketData.load(file);
	}

}