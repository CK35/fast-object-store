package de.ck35.metricstore.util;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DayBasedIntervalSplitterTest {

	@Test
	public void testIteratorWithDifferentZone() {
		DateTimeZone zone = DateTimeZone.forID("Europe/Berlin");
		DayBasedIntervalSplitter splitter = new DayBasedIntervalSplitter(new Interval(new DateTime(2015, 1, 1, 0, 0, zone),
																	  new DateTime(2015, 1, 2, 0, 0, zone)));
		Interval interval1 = new Interval(new DateTime(2014, 12, 31, 23, 0, DateTimeZone.UTC), 
									      new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC));
		
		Interval interval2 = new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), 
			      						  new DateTime(2015, 1, 1, 23, 0, DateTimeZone.UTC));
		
		assertEquals(ImmutableList.of(interval1, interval2), ImmutableList.copyOf(splitter));
	}

}