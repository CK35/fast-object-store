package de.ck35.metricstore.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.metricstore.fs.BucketData;
import de.ck35.metricstore.fs.FilesystemBucket;
import de.ck35.metricstore.fs.ObjectNodeReader;
import de.ck35.metricstore.fs.ObjectNodeWriter;
import de.ck35.metricstore.util.DateTimeFunction;
import de.ck35.metricstore.util.LRUCache;

public class FilesystemBucketTest {

	private static final Logger LOG = LoggerFactory.getLogger(FilesystemBucketTest.class);
	
	private BucketData bucketData;
	private Function<ObjectNode, DateTime> timestampFunction;
	private Function<Path, ObjectNodeWriter> writerFactory;
	private Function<Path, ObjectNodeReader> readerFactory;
	private LRUCache<Path, ObjectNodeWriter> writers;
	
	public FilesystemBucketTest() throws IOException {
		this.bucketData = new BucketData(Files.createTempDirectory("FilesystemBucketTest"), "TestBucket", "TestBucketType");
		this.timestampFunction = new DateTimeFunction();
	}
	
	@After
	public void after() {
		
	}
	
	@Test
	public void testWrite() {
		fail("Not yet implemented");
	}

	@Test
	public void testExpand() {
		FilesystemBucket bucket = new FilesystemBucket(bucketData, timestampFunction, writerFactory, readerFactory, writers);
		fail("Not yet implemented");
	}

	@Test
	public void testClearDirectory() throws IOException {
		Path directory = Files.createTempDirectory("clearTest");
		LOG.debug("Using dir: '{}' for clear test.", directory);
		Path file1 = Files.createFile(directory.resolve("file1"));
		Path subDir = Files.createDirectory(directory.resolve("dir2"));
		Path file2 = Files.createFile(subDir.resolve("file2"));
		FilesystemBucket.clearDirectory(directory);
		assertTrue(Files.isDirectory(directory));
		assertFalse(Files.isRegularFile(file1));
		assertFalse(Files.isDirectory(subDir));
		assertFalse(Files.isRegularFile(file2));
		Files.delete(directory);
	}

}