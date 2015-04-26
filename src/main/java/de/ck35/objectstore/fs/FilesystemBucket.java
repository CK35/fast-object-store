package de.ck35.objectstore.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.objectstore.api.Bucket;
import de.ck35.objectstore.api.StoredObjectNode;

public class FilesystemBucket implements Bucket, Closeable {
	
	private static final String DAY_FILE_SUFFIX = ".day";

	private static final Logger LOG = LoggerFactory.getLogger(FilesystemBucket.class);

	private final BucketData bucketData;
	
	private final Map<Path, ObjectNodeWriter> writers;
	private final Function<ObjectNode, DateTime> timestampFunction;
	private final Function<Path, ObjectNodeWriter> writerFactory;
	private final Function<Path, ObjectNodeReader> readerFactory;

	public FilesystemBucket(BucketData bucketData,
	                  Function<ObjectNode, DateTime> timestampFunction,
	                  Function<Path, ObjectNodeWriter> writerFactory,
	                  Function<Path, ObjectNodeReader> readerFactory) {
		this.bucketData = bucketData;
		this.timestampFunction = timestampFunction;
		this.writerFactory = writerFactory;
		this.readerFactory = readerFactory;
		this.writers = new HashMap<>();
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
		for(ObjectNodeWriter writer : writers.values()) {
			try {				
				writer.close();
			} catch (IOException e) {
				LOG.warn("Error while closing writer for path: '{}'.", writer.getPath(), e);
			}
		}
	}
	
	public StoredObjectNode write(DateTime timestamp, ObjectNode objectNode) throws IOException {
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
		try(StoredObjectNodeReader reader = new StoredObjectNodeReader(this, readerFactory.apply(dayFile), timestampFunction)) {
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