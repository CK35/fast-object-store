package de.ck35.objectstore.fs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesystemBucketTest {

	private static final Logger LOG = LoggerFactory.getLogger(FilesystemBucketTest.class);
	
	@Test
	public void testWrite() {
		fail("Not yet implemented");
	}

	@Test
	public void testExpand() {
		fail("Not yet implemented");
	}

	@Test
	public void testResolveMinuteFile() {
		fail("Not yet implemented");
	}

	@Test
	public void testResolveDayFile() {
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
