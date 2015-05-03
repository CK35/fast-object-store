package de.ck35.metricstore.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;
import de.ck35.metricstore.util.LRUCache;
import de.ck35.metricstore.util.MetricsIOException;

public class FilesystemBucket implements MetricBucket, Closeable {
	
	private static final String DAY_FILE_SUFFIX = ".day";
	private static final String TMP_SUFFIX = "-tmp";

	private static final Logger LOG = LoggerFactory.getLogger(FilesystemBucket.class);

	private final BucketData bucketData;
	
	private final LRUCache<Path, ObjectNodeWriter> writers;
	private final Function<ObjectNode, DateTime> timestampFunction;
	private final Function<Path, ObjectNodeWriter> writerFactory;
	private final Function<Path, ObjectNodeReader> readerFactory;

	public FilesystemBucket(BucketData bucketData,
	                  	    Function<ObjectNode, DateTime> timestampFunction,
	                  	    Function<Path, ObjectNodeWriter> writerFactory,
	                  	    Function<Path, ObjectNodeReader> readerFactory,
	                  	    LRUCache<Path, ObjectNodeWriter> writers) {
		this.bucketData = bucketData;
		this.timestampFunction = timestampFunction;
		this.writerFactory = writerFactory;
		this.readerFactory = readerFactory;
		this.writers = writers;
	}
	
	@Override
	public String getName() {
		return bucketData.getName();
	}
	@Override
	public String getType() {
		return bucketData.getType();
	}
	public BucketData getBucketData() {
		return bucketData;
	}
	
	@Override
	public void close() throws IOException {
		for(ObjectNodeWriter writer : writers) {
			try {				
				writer.close();
			} catch (IOException e) {
				LOG.warn("Error while closing writer for path: '{}'.", writer.getPath(), e);
			}
		}
		writers.clear();
	}
	
	public void read(Interval interval, StoredMetricCallable callable) {
		try {
			DateTime start = interval.getStart().withZone(DateTimeZone.UTC);
			DateTime end = interval.getEnd().withZone(DateTimeZone.UTC);
			for(DateTime current = start ; current.isBefore(end) ; current = current.plusMinutes(1)) {
				Path dayFile = resolveDayFile(current);
				if(Files.isRegularFile(dayFile)) {
					try(StoredObjectNodeReader reader = createReader(dayFile)) {
						current = read(current, end, reader, callable);
					}
				} else {
					Path minuteFile = resolveMinuteFile(current);
					if(Files.isRegularFile(minuteFile)) {
						ObjectNodeWriter writer = writers.remove(minuteFile);
						if(writer != null) {
							writer.close();
						}
						try(StoredObjectNodeReader reader = createReader(minuteFile)) {
							current = read(current, end, reader, callable);
						}
					}
				}
			}
		} catch(IOException e) {
			throw new MetricsIOException("Could not close a resource while reading from bucket: '" + bucketData + "'!", e);
		}
	}
	
	protected DateTime read(DateTime start, DateTime end, StoredObjectNodeReader reader, StoredMetricCallable callable) {
		DateTime current = start;
		for(StoredMetric next = reader.read() ; next != null ; next = reader.read()) {
			current = next.getTimestamp();
			if(current.isBefore(start)) {
				continue;
			}
			if(current.isEqual(end) || current.isAfter(end)) {
				return current;
			}
			current = next.getTimestamp();
			callable.call(next);
		}
		return current;
	}
	
	protected StoredObjectNodeReader createReader(Path path) {
		return new StoredObjectNodeReader(this, readerFactory.apply(path), timestampFunction);
	}
	
	public StoredMetric write(ObjectNode objectNode) {
		DateTime timestamp = Objects.requireNonNull(timestampFunction.apply(objectNode)).withZone(DateTimeZone.UTC);
		Path minuteFile = resolveMinuteFile(timestamp);
		ObjectNodeWriter writer = writers.get(minuteFile);
		if(writer == null) {
			Path dayFile = resolveDayFile(timestamp);
			if(Files.isRegularFile(dayFile)) {
				expand(dayFile, minuteFile);
			}
			try {
				Files.createDirectories(minuteFile.getParent());
			} catch (IOException e) {
				throw new MetricsIOException("Could not create day directories for minute file: '" + minuteFile + "'!", e);
			}
			writer = writerFactory.apply(minuteFile);
			writers.put(minuteFile, writer);
		}
		writer.write(objectNode);
		return StoredObjectNodeReader.storedObjectNode(this, timestamp, objectNode);
	}
	
	public void expand(Path dayFile, Path minuteFile) {
		Path tmpDir = resolveTMPDayFolder(minuteFile);
		if(Files.isDirectory(tmpDir)) {			
			clearDirectory(tmpDir);
		} else {
			try {
				Files.createDirectories(tmpDir);
			} catch (IOException e) {
				throw new MetricsIOException("Could not create tmp directory: '" + tmpDir + "' for day file expansion!", e);
			}
		}
		try {
			try(StoredObjectNodeReader reader = createReader(dayFile)) {
				ObjectNodeWriter writer = null;
				try {
					for(StoredMetric storedNode = reader.read(); storedNode != null ; storedNode = reader.read()) {
						Path path = resolveMinuteFileInsideDayDir(tmpDir, storedNode.getTimestamp());
						if(writer == null || !path.equals(writer.getPath())) {
							if(writer != null) {
								writer.close();
							}
							writer = writerFactory.apply(path);
						}
						writer.write(storedNode.getObjectNode());
					}
				} finally {
					if(writer != null) {
						writer.close();
					}
				}
			}
		} catch(IOException e) {
			throw new MetricsIOException("Expanding day file: '" + dayFile + "' failed. Could not close a resource!", e);
		}
		try {			
			Files.move(tmpDir, minuteFile.getParent());
		} catch(IOException e) {
			throw new MetricsIOException("Expanding day file: '" + dayFile + "' failed. Renaming tmp day folder failed!", e);
		}
		try {			
			Files.delete(dayFile);
		} catch(IOException e) {
			throw new MetricsIOException("Expanding day file: '" + dayFile + "' failed. Deleting old day file failed!", e);
		}
	}
	
	public void compressAll(LocalDate until) {
		Function<Path, Integer> pathToIntFunction = new PathToIntFunction(); 
		for(Path yearDir : listChildsSortedByNumericName(bucketData.getBasePath(), true)) {
			for(Path monthDir : listChildsSortedByNumericName(bucketData.getBasePath(), true)) {
				for(Path dayDir : listChildsSortedByNumericName(bucketData.getBasePath(), true)) {
					LocalDate currentDay = new LocalDate(pathToIntFunction.apply(yearDir), 
					                                     pathToIntFunction.apply(monthDir), 
					                                     pathToIntFunction.apply(dayDir));
					if(currentDay.isBefore(until)) {
						compress(dayDir);
					}
				}
			}
		}
	}
	
	public void compress(Path dayDir) {
		Path tmpDayFile = resolveTMPDayFile(dayDir);
		Path dayFile = resolveDayFile(dayDir);
		try(OutputStream out = Files.newOutputStream(tmpDayFile);
			GZIPOutputStream gzout = new GZIPOutputStream(new BufferedOutputStream(out))) {
			for(Path minuteFile : listChildsSortedByNumericName(dayDir, false)) {
				ObjectNodeWriter writer = writers.remove(minuteFile);
				if(writer != null) {
					writer.close();
				}
				try(InputStream in = Files.newInputStream(minuteFile);
					GZIPInputStream gzin = new GZIPInputStream(new BufferedInputStream(in))) {
					for(int next = gzin.read() ; next != -1 ; next = gzin.read()) {
						gzout.write(next);
					}
				}
			}
		} catch(IOException e) {
			throw new MetricsIOException("Could not create compressed day file: '" + tmpDayFile + "'!", e);
		}
		try {
			Files.move(tmpDayFile, dayFile, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			throw new MetricsIOException("Could not rename tmp day file: '" + tmpDayFile + "' to: '" + dayFile + "'!", e);
		}
		clearDirectory(dayDir);
		try {
			Files.delete(dayDir);
		} catch (IOException e) {
			throw new MetricsIOException("Could not delete old day folder: '" + dayDir + "'!", e);
		}
	}
	
	public Path resolveMinuteFile(DateTime timestamp) {
		return resolveMinuteFileInsideDayDir(bucketData.getBasePath().resolve(Integer.toString(timestamp.getYear()))
											   						 .resolve(Integer.toString(timestamp.getMonthOfYear()))
											   						 .resolve(Integer.toString(timestamp.getDayOfMonth())), timestamp);
	}
	
	protected Path resolveMinuteFileInsideDayDir(Path dayDir, DateTime timestamp) {
		return dayDir.resolve(Integer.toString(timestamp.getMinuteOfDay()));
	}
	
	public Path resolveDayFile(DateTime timestamp) {
		return bucketData.getBasePath().resolve(Integer.toString(timestamp.getYear()))
				   					   .resolve(Integer.toString(timestamp.getMonthOfYear()))
				   					   .resolve(Integer.toString(timestamp.getDayOfMonth()) + DAY_FILE_SUFFIX);
	}
	
	protected Path resolveTMPDayFolder(Path minuteFile) {
		return minuteFile.getParent().getParent().resolve(minuteFile.getParent().getFileName() + TMP_SUFFIX);
	}
	
	protected Path resolveDayFile(Path dayFolder) {
		return dayFolder.getParent().resolve(dayFolder.getFileName() + DAY_FILE_SUFFIX);
	}
	
	protected Path resolveTMPDayFile(Path dayFolder) {
		return resolveDayFile(dayFolder).getParent().resolve(dayFolder.getFileName() + TMP_SUFFIX);
	}
	
	/**
	 * Delete all files and folders inside the given root directory. The given directory
	 * will not be deleted.
	 * 
	 * @param root The directory to clean.
	 */
	public static void clearDirectory(final Path root) {
		try {			
			Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}
				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exception) throws IOException {
					if(exception != null) {					
						throw exception;
					}
					if(!root.equals(dir)) {					
						Files.delete(dir);
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} catch(IOException e) {
			throw new MetricsIOException("Could not clean direcotry: '" + root + "'!", e);
		}
	}
	
	public static List<Path> listChildsSortedByNumericName(Path parent, boolean directories) {
		Function<Path, Integer> function = new PathToIntFunction();
		Filter<Path> filter = new IntBasedFileTypePredicate(directories, function);
		try(DirectoryStream<Path> stream = Files.newDirectoryStream(parent, filter)) {
			List<Path> content = Lists.newArrayList(stream);
			Collections.sort(content, new IntBasedComparator(function));
			return content;
		} catch(IOException e) {
			throw new MetricsIOException("Could not list content of: '" + parent + "'!", e);
		}
	}
	
	public static class PathToIntFunction implements Function<Path, Integer> {
		@Override
		public Integer apply(Path input) {
			return input == null ? null : Integer.parseInt(input.getFileName().toString());
		}
	}
	public static class IntBasedFileTypePredicate implements Filter<Path> {
		
		private final boolean directories;
		private final Function<Path, Integer> intFunction;
		
		public IntBasedFileTypePredicate(boolean directories, Function<Path, Integer> intFunction) {
			this.directories = directories;
			this.intFunction = intFunction;
		}
		@Override
		public boolean accept(Path entry) throws IOException {
			if((directories && Files.isDirectory(entry)) || (!directories && Files.isRegularFile(entry))) {
				try {						
					intFunction.apply(entry);
					return true;
				} catch(NumberFormatException e) {
					return false;
				}
			}
			return false;
		}
	}
	public static class IntBasedComparator implements Comparator<Path> {
		
		private final Function<Path, Integer> intFunction;
		
		public IntBasedComparator(Function<Path, Integer> intFunction) {
			this.intFunction = intFunction;
		}
		@Override
		public int compare(Path o1, Path o2) {
			return Integer.compare(intFunction.apply(o1), intFunction.apply(o2));
		}
	}
}