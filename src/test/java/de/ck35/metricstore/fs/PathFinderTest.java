package de.ck35.metricstore.fs;

import static org.junit.Assert.*;

import java.nio.file.Paths;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;

public class PathFinderTest {

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

}