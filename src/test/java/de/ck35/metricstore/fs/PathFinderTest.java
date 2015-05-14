package de.ck35.metricstore.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@RunWith(MockitoJUnitRunner.class)
public class PathFinderTest {

	private Path exampleBucketPath;
	
	@Before
	public void before() throws URISyntaxException {
		URL url = getClass().getClassLoader().getResource("example-bucket");
		assertNotNull(url);
		exampleBucketPath = Paths.get(url.toURI());
		assertTrue(Files.isDirectory(exampleBucketPath));
	}
	
	@Test
	public void testPathFinderLocalDatePath() {
		PathFinder pathFinder = new PathFinder(new LocalDate(2015, 1, 1), Paths.get(""));
		assertEquals(Paths.get("2015","1","1","0"), pathFinder.getMinuteFilePath());
	}

	@Test
	public void testGetMinuteFilePath() {
		PathFinder pathFinder = new PathFinder(new DateTime(2015, 1, 1, 0, 0), Paths.get(""));
		assertEquals(Paths.get("2015","1","1","0"), pathFinder.getMinuteFilePath());
	}

	@Test
	public void testGetTemporaryMinuteFilePath() {
		PathFinder pathFinder = new PathFinder(new DateTime(2015, 1, 1, 0, 0), Paths.get(""));
		assertEquals(Paths.get("2015","1","1-tmp","0"), pathFinder.getTemporaryMinuteFilePath());
	}

	@Test
	public void testGetDayFilePath() {
		PathFinder pathFinder = new PathFinder(new DateTime(2015, 1, 1, 0, 0), Paths.get(""));
		assertEquals(Paths.get("2015","1","1.day"), pathFinder.getDayFilePath());
	}

	@Test
	public void testGetTemporaryDayFilePath() {
		PathFinder pathFinder = new PathFinder(new DateTime(2015, 1, 1, 0, 0), Paths.get(""));
		assertEquals(Paths.get("2015","1","1.day-tmp"), pathFinder.getTemporaryDayFilePath());
	}
	
	@Test
	public void testGetTimestamp() {
		PathFinder pathFinder = new PathFinder(new LocalDate(2015, 1, 1), exampleBucketPath);
		assertEquals(new LocalDate(2015, 1, 1), pathFinder.getDate());
		assertEquals(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), pathFinder.getTimestamp());
	}
	
	@Test
	public void testIterateDays() throws URISyntaxException {
		PathFinder pathFinder = new PathFinder(new LocalDate(2015, 1, 1), exampleBucketPath);
		List<DateTime> timestamps = ImmutableList.copyOf(Iterables.transform(pathFinder, new DateTimeExtractor()));
		List<Path> pathList = ImmutableList.copyOf(Iterables.transform(pathFinder, new BasePathExtractor()));
		assertEquals(ImmutableList.of(new DateTime(2014, 12, 2, 0, 0, DateTimeZone.UTC),
									  new DateTime(2014, 12, 10, 0, 0, DateTimeZone.UTC),
									  new DateTime(2014, 12, 11, 0, 0, DateTimeZone.UTC),
									  new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC),
									  new DateTime(2015, 1, 2, 0, 0, DateTimeZone.UTC)), timestamps);
		assertEquals(ImmutableList.of(exampleBucketPath,
									  exampleBucketPath,
									  exampleBucketPath,
									  exampleBucketPath,
									  exampleBucketPath), pathList);
	}
	
	@Test
	public void testiIterateMinutesOfDay() {
		PathFinder pathFinder = new PathFinder(new LocalDate(2015, 1, 1), exampleBucketPath);
		Iterable<PathFinder> minutesOfDay = pathFinder.iterateMinutesOfDay();
		List<DateTime> timestamps = ImmutableList.copyOf(Iterables.transform(minutesOfDay, new DateTimeExtractor()));
		List<Path> pathList = ImmutableList.copyOf(Iterables.transform(minutesOfDay, new BasePathExtractor()));
		assertEquals(ImmutableList.of(new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC),
									  new DateTime(2015, 1, 1, 0, 2, DateTimeZone.UTC),
									  new DateTime(2015, 1, 1, 0, 10, DateTimeZone.UTC)), timestamps);
		assertEquals(ImmutableList.of(exampleBucketPath,
									  exampleBucketPath,
									  exampleBucketPath), pathList);
	}
	
	public static class BasePathExtractor implements Function<PathFinder, Path> {
		@Override
		public Path apply(PathFinder input) {
			return input.getBasePath();
		}
	}
	public static class DateTimeExtractor implements Function<PathFinder, DateTime> {
		@Override
		public DateTime apply(PathFinder input) {
			return input.getTimestamp();
		}
	}
	
}