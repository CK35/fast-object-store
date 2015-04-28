package de.ck35.objectstore.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.objectstore.api.Bucket;
import de.ck35.objectstore.api.StoredObjectNode;
import de.ck35.objectstore.api.StoredObjectNodeCallable;

public class FilesystemBucket implements Bucket, Closeable {
	
	private static final String DAY_FILE_SUFFIX = ".day";

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
	
	public void read(Interval interval, StoredObjectNodeCallable callable) throws IOException {
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
	}
	
	protected DateTime read(DateTime start, DateTime end, StoredObjectNodeReader reader, StoredObjectNodeCallable callable) throws IOException {
		DateTime current = start;
		for(StoredObjectNode next = reader.read() ; next != null ; next = reader.read()) {
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
	
	public StoredObjectNode write(ObjectNode objectNode) throws IOException {
		DateTime timestamp = Objects.requireNonNull(timestampFunction.apply(objectNode)).withZone(DateTimeZone.UTC);
		Path minuteFile = resolveMinuteFile(timestamp);
		ObjectNodeWriter writer = writers.get(minuteFile);
		if(writer == null) {
			Path dayFile = resolveDayFile(timestamp);
			if(Files.isRegularFile(dayFile)) {
				expand(dayFile, minuteFile);
			}
			writer = writerFactory.apply(minuteFile);
			writers.put(minuteFile, writer);
		}
		writer.write(objectNode);
		return StoredObjectNodeReader.storedObjectNode(this, timestamp, objectNode);
	}
	
	public void expand(Path dayFile, Path minuteFile) throws IOException {
		Path tmpDir = minuteFile.getParent().getParent().resolve(minuteFile.getParent() + "-tmp");
		if(Files.isDirectory(tmpDir)) {			
			try {
				clearDirectory(tmpDir);
			} catch (IOException e) {
				throw new IOException("Could not cleanup existing tmp dir:" + tmpDir + " for minute file expansion!", e);
			}
		}
		try(StoredObjectNodeReader reader = createReader(dayFile)) {
			ObjectNodeWriter writer = null;
			try {
				for(StoredObjectNode storedNode = reader.read(); storedNode != null ; storedNode = reader.read()) {
					Path path = resolveMinuteFile(storedNode.getTimestamp());
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
		Files.move(tmpDir, minuteFile.getParent());
		Files.delete(dayFile);
	}
	
	public Path resolveMinuteFile(DateTime timestamp) {
		return bucketData.getBasePath().resolve(Integer.toString(timestamp.getYear()))
									   .resolve(Integer.toString(timestamp.getMonthOfYear()))
									   .resolve(Integer.toString(timestamp.getDayOfMonth()))
									   .resolve(Integer.toString(timestamp.getMinuteOfDay()));
	}
	
	public Path resolveDayFile(DateTime timestamp) {
		return bucketData.getBasePath().resolve(Integer.toString(timestamp.getYear()))
				   					   .resolve(Integer.toString(timestamp.getMonthOfYear()))
				   					   .resolve(Integer.toString(timestamp.getDayOfMonth()) + DAY_FILE_SUFFIX);
	}
	
	public static void clearDirectory(final Path root) throws IOException {
		Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
				if( e != null) {					
					throw e;
				}
				if(!root.equals(dir)) {					
					Files.delete(dir);
				}
				return FileVisitResult.CONTINUE;
			}
		});
	}
}