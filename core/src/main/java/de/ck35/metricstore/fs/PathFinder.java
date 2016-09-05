package de.ck35.metricstore.fs;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import de.ck35.metricstore.fs.NumericSortedPathIterable.DefaultNumberFunction;

/**
 * Responsible for all file paths which are used for storing data and temporary data.
 * Every returned path is a child path of the provided base path. 
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class PathFinder implements Iterable<PathFinder>, Function<Entry<Integer, Path>, PathFinder> {
	
	private static final String DAY_FILE_SUFFIX = ".day";
	private static final String TMP_SUFFIX = "-tmp";
	
	private final DateTime timestamp;
	private final Path basePath;
	
	/**
	 * Construct the finder from the given date.
	 * 
	 * @param date The date.
	 * @param basePath The base path.
	 */
	public PathFinder(LocalDate date, Path basePath) {
		this(new DateTime(date.getYear(), 
						  date.getMonthOfYear(), 
						  date.getDayOfMonth(), 0, 0, DateTimeZone.UTC), basePath);
	}
	
	/**
	 * Construct the finder from the given timestamp.
	 * 
	 * @param timestamp The timestamp.
	 * @param basePath The base path.
	 */
	public PathFinder(DateTime timestamp, Path basePath) {
		this.timestamp = Objects.requireNonNull(timestamp);
		this.basePath = Objects.requireNonNull(basePath);
	}
	
	/**
	 * @return The path of a minute file. This file is the smallest chunk of data.
	 */
	public Path getMinuteFilePath() {
		return getDayDirectoryPath().resolve(Integer.toString(timestamp.getMinuteOfDay()));
	}
	
	/**
	 * @return The path of the folder where minute files are stored.
	 */
	public Path getDayDirectoryPath() {
		return basePath.resolve(Integer.toString(timestamp.getYear()))
				   .resolve(Integer.toString(timestamp.getMonthOfYear()))
				   .resolve(Integer.toString(timestamp.getDayOfMonth()));
	}
	
	/**
	 * @return The path of the temporary minute file which is used while expanding a day file back into single minute files.
	 */
	public Path getTemporaryMinuteFilePath() {
		Path minuteFilePath = getMinuteFilePath();
		return minuteFilePath.getParent().getParent().resolve(minuteFilePath.getParent().getFileName().toString() + TMP_SUFFIX)
													 .resolve(minuteFilePath.getFileName());
	}
	
	/**
	 * @return The path of the day file. This file holds all data for one day (content of all minute files of one day). 
	 */
	public Path getDayFilePath() {
		return basePath.resolve(Integer.toString(timestamp.getYear()))
					   .resolve(Integer.toString(timestamp.getMonthOfYear()))
					   .resolve(Integer.toString(timestamp.getDayOfMonth()) + DAY_FILE_SUFFIX);
	}
	
	/**
	 * @return The path of the temporary day file which is used while compressing minute files into one day file.
	 */
	public Path getTemporaryDayFilePath() {
		Path dayFilePath = getDayFilePath();
		return dayFilePath.getParent().resolve(dayFilePath.getFileName().toString() + TMP_SUFFIX);
	}
	
	public DateTime getTimestamp() {
		return timestamp;
	}
	public LocalDate getDate() {
		return timestamp.toLocalDate();
	}
	public Path getBasePath() {
		return basePath;
	}
	
	@Override
	public PathFinder apply(Entry<Integer, Path> input) {
		return new PathFinder(timestamp.withMillisOfDay((int)TimeUnit.MILLISECONDS.convert(input.getKey(), TimeUnit.MINUTES)), 
							  basePath);
	}
	@Override
	public Iterator<PathFinder> iterator() {
		return new PathIterator(basePath);
	}
	
	public Iterable<PathFinder> iterateMinutesOfDay() {
		return Iterables.transform(new NumericSortedPathIterable(getDayDirectoryPath()), this);
	}
	
	private static class PathIterator extends AbstractIterator<PathFinder> implements Function<Path, Integer> {
		
		private final Path basePath;

		private Iterator<Entry<Integer, Path>> yearIter;
		private Iterator<Entry<Integer, Path>> monthIter;
		private Iterator<Entry<Integer, Path>> dayIter;
		
		private Entry<Integer, Path> currentYear;
		private Entry<Integer, Path> currentMonth;
		
		public PathIterator(Path basePath) {
			this.basePath = basePath;
		}
		
		@Override
		protected PathFinder computeNext() {
			while(dayIter == null || !dayIter.hasNext()) {
				while(monthIter == null || !monthIter.hasNext()) {
					if(yearIter == null) {
						yearIter = new NumericSortedPathIterable(basePath).iterator();
					}
					if(!yearIter.hasNext()) {
						return endOfData();
					}
					currentYear = yearIter.next();
					monthIter = new NumericSortedPathIterable(currentYear.getValue()).iterator();
				}
				currentMonth = monthIter.next();
				dayIter = new NumericSortedPathIterable(currentMonth.getValue(), this).iterator();
			}
			Entry<Integer, Path> day = dayIter.next();
			LocalDate date = new LocalDate(currentYear.getKey(), currentMonth.getKey(), day.getKey());
			return new PathFinder(date, basePath);
		}

		@Override
		public Integer apply(Path input) {
			String fileName = input.getFileName().toString();
			if(fileName.endsWith(DAY_FILE_SUFFIX)) {
				return Integer.parseInt(fileName.substring(0, fileName.length() - DAY_FILE_SUFFIX.length()));
			} else {
				return DefaultNumberFunction.intFromFileName(input);
			}
		}
	}
}