package de.ck35.metricstore.fs;

import java.nio.file.Path;
import java.util.Objects;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

/**
 * Responsible for all file paths which are used for storing data and temporary data.
 * Every returned path is a child path of the provided base path. 
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class PathFinder {
	
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
		return basePath.resolve(Integer.toString(timestamp.getYear()))
					   .resolve(Integer.toString(timestamp.getMonthOfYear()))
					   .resolve(Integer.toString(timestamp.getDayOfMonth()))
					   .resolve(Integer.toString(timestamp.getMinuteOfDay()));
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
	
}