package de.ck35.metricstore.fs;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import de.ck35.metricstore.fs.NumericSortedPathIterable.DefaultNumberFunction;
import de.ck35.metricstore.util.MetricsIOException;

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
	public void testIterable() throws URISyntaxException {
		assertEquals(ImmutableList.of(Maps.immutableEntry(1, basePath.resolve("1")),
									  Maps.immutableEntry(3, basePath.resolve("3")),
									  Maps.immutableEntry(5, basePath.resolve("5")),
									  Maps.immutableEntry(10, basePath.resolve("10"))), 
									  ImmutableList.copyOf(new NumericSortedPathIterable(basePath)));
	}
	
	@Test(expected=MetricsIOException.class)
	public void testIterableOnFile() {
		ImmutableList.copyOf(new NumericSortedPathIterable(basePath.resolve("1")));
	}
	
	@Test
	public void testApplyNullPath() {
		NumericSortedPathIterable iterable = new NumericSortedPathIterable(basePath);
		assertNull(iterable.apply(null));
	}
	
	@Test
	public void testDefaultNumberFunctionApplyNull() {
		DefaultNumberFunction function = new DefaultNumberFunction();
		assertNull(function.apply(null));
	}
}