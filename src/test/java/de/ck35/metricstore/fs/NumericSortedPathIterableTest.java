package de.ck35.metricstore.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class NumericSortedPathIterableTest {

	private Path basePath;
	
	@Before
	public void before() throws URISyntaxException {
		URL url = getClass().getClassLoader().getResource("list-test-files/1");
		assertNotNull(url);
		basePath = Paths.get(url.toURI()).getParent();
		assertTrue(Files.isDirectory(basePath));
	}
	
	@Test
	public void testNumericSortedPathChildsWithDirectories() throws URISyntaxException {
		assertEquals(ImmutableList.of(basePath.resolve("3"),
		                              basePath.resolve("5"),
		                              basePath.resolve("40")), ImmutableList.copyOf(new NumericSortedPathIterable(basePath, true)));
	}
	
	@Test
	public void testNumericSortedPathChildsWithFiles() throws URISyntaxException {
		assertEquals(ImmutableList.of(basePath.resolve("1"),
		                              basePath.resolve("2"),
		                              basePath.resolve("10")), ImmutableList.copyOf(new NumericSortedPathIterable(basePath, false)));
	}

}