package de.ck35.metricstore.util;

import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.google.common.collect.AbstractIterator;

/**
 * Splits an interval into smaller intervals which do not overlap two days. If you have
 * an interval starting from one day 23:00 until next day 02:00, this will result in
 * two intervals. The first starts at 23:00 and ends at 00:00. The second starts at
 * 00:00 and ends at 02:00.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class DayBasedIntervalSplitter implements Iterable<Interval> {

	private final Interval interval;

	public DayBasedIntervalSplitter(Interval interval) {
		this.interval = new Interval(interval.getStart().withZone(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0), 
									 interval.getEnd().withZone(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0));
	}

	@Override
	public Iterator<Interval> iterator() {
		return new IntervalIterator(interval);
	}
	
	public class IntervalIterator extends AbstractIterator<Interval> {
		
		private Interval pending;
		
		public IntervalIterator(Interval interval) {
			pending = interval;
		}
		@Override
		protected Interval computeNext() {
			if(pending.toDuration().isEqual(Duration.ZERO)) {
				return endOfData();
			}
			final Interval result;
			DateTime endOfDay = pending.getStart().withMillisOfDay(0).plusDays(1);
			if(endOfDay.isBefore(pending.getEnd())) {
				result = new Interval(pending.getStart(), endOfDay);
				pending = new Interval(endOfDay, pending.getEnd());
			} else {
				result = pending;
				pending = new Interval(pending.getEnd(), pending.getEnd());
			}
			return result;
		}
	}
}