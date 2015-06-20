package de.ck35.metricstore.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.util.LRUCache;
import de.ck35.metricstore.util.MetricsIOException;

/**
 * {@link MetricBucket} implementation which allows writing, deleting and compressing. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class WritableFilesystemBucket extends ReadableFilesystemBucket implements Closeable {
	
	private static final Logger LOG = LoggerFactory.getLogger(WritableFilesystemBucket.class);

	private final LRUCache<Path, ObjectNodeWriter> writers;
	private final Function<ObjectNode, DateTime> timestampFunction;
	private final Function<Path, ObjectNodeWriter> writerFactory;

	public WritableFilesystemBucket(BucketData bucketData,
	                  	    		Function<ObjectNode, DateTime> timestampFunction,
	                  	    		Function<Path, ObjectNodeWriter> writerFactory,
	                  	    		Function<Path, ObjectNodeReader> readerFactory,
	                  	    		LRUCache<Path, ObjectNodeWriter> writers) {
		super(bucketData, timestampFunction, readerFactory);
		this.timestampFunction = timestampFunction;
		this.writerFactory = writerFactory;
		this.writers = writers;
	}
	
	@Override
	public void close() throws IOException {
		for(ObjectNodeWriter writer : writers) {
			try {				
			    Thread.interrupted();
				writer.close();
			} catch (IOException e) {
				LOG.warn("Error while closing writer for path: '{}'.", writer.getPath(), e);
			}
		}
		writers.clear();
	}
	
	@Override
	protected StoredObjectNodeReader createReader(Path path) {
		ObjectNodeWriter writer = writers.remove(path);
		if(writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				throw new MetricsIOException("Could not close writer for path: '" + path + "'!", e);
			}
		}
		return super.createReader(path);
	}
	
	public StoredMetric write(ObjectNode objectNode) {
		DateTime timestamp = Objects.requireNonNull(timestampFunction.apply(objectNode));
		PathFinder pathFinder = pathFinder(timestamp);
		Path minuteFile = pathFinder.getMinuteFilePath();
		ObjectNodeWriter writer = writers.get(minuteFile);
		if(writer == null) {
			Path dayFile = pathFinder.getDayFilePath();
			if(Files.isRegularFile(dayFile)) {
				expand(pathFinder);
			}
			try {
				Files.createDirectories(minuteFile.getParent());
			} catch (IOException e) {
				throw new MetricsIOException("Could not create day directories for minute file: '" + minuteFile + "'!", e);
			}
			writer = writerFactory.apply(minuteFile);
			IOException firstCloseException = null;
			for(ObjectNodeWriter oldWriter : writers.put(minuteFile, writer)) {
			    try {
                    oldWriter.close();
                } catch (IOException e) {
                    if(firstCloseException == null) {
                        firstCloseException = e;
                    }
                }
			}
			if(firstCloseException != null) {			    
			    throw new MetricsIOException("Could not close old writer!", firstCloseException);
			}
		}
		writer.write(objectNode);
		return StoredObjectNodeReader.storedObjectNode(this, timestamp, objectNode);
	}
	
	public void expand(PathFinder parentPathFinder) {
		Path dayFile = parentPathFinder.getDayFilePath();
		Path tmpDir = parentPathFinder.getTemporaryMinuteFilePath().getParent();
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
						PathFinder pathFinder = pathFinder(storedNode.getTimestamp());
						Path path = pathFinder.getTemporaryMinuteFilePath();
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
			Files.move(tmpDir, parentPathFinder.getMinuteFilePath().getParent());
		} catch(IOException e) {
			throw new MetricsIOException("Expanding day file: '" + dayFile + "' failed. Renaming tmp day folder failed!", e);
		}
		try {			
			Files.delete(dayFile);
		} catch(IOException e) {
			throw new MetricsIOException("Expanding day file: '" + dayFile + "' failed. Deleting old day file failed!", e);
		}
	}
	
	public void deletAll(final LocalDate until) {
		for(PathFinder pathFinder : pathFinder(until)) {
			if(pathFinder.getDate().isBefore(until)) {
				delete(pathFinder);
			} else {
				break;
			}
		}
	}
	
	public void delete(PathFinder pathFinder) {
		Path dayFilePath = pathFinder.getDayFilePath();
		if(Files.isRegularFile(dayFilePath)) {
			try {
				Files.delete(dayFilePath);
			} catch (IOException e) {
				throw new MetricsIOException("Could not delete day file: '" + dayFilePath + "'!", e);
			}
		} else {
			Path dayDirectoryPath = pathFinder.getDayDirectoryPath();
			for(PathFinder minuteOfDay : pathFinder.iterateMinutesOfDay()) {
				Path minuteFilePath = minuteOfDay.getMinuteFilePath();
				try {
					closeWriter(minuteFilePath);
				} catch (IOException e) {
					throw new MetricsIOException("Could not delete day folder because writer: '" + minuteFilePath + "' could not be closed!", e);
				}
			}
			clearDirectory(dayDirectoryPath);
			try {
				Files.delete(dayDirectoryPath);
			} catch (IOException e) {
				throw new MetricsIOException("Could not delete day folder: '" + dayDirectoryPath + "'!", e);
			}
		}
	}
	
	public void compressAll(final LocalDate until) {
		for(PathFinder pathFinder : pathFinder(until)) {
			if(pathFinder.getDate().isBefore(until)) {
				compress(pathFinder);
			} else {
				break;
			}
		}
	}
	
	public void compress(PathFinder pathFinder) {
		Path dayFile = pathFinder.getDayFilePath();
		if(Files.isRegularFile(dayFile)) {
			return;
		}
		Path dayDir = pathFinder.getDayDirectoryPath();
		Path tmpDayFile = pathFinder.getTemporaryDayFilePath();
		try(OutputStream out = Files.newOutputStream(tmpDayFile);
			GZIPOutputStream gzout = new GZIPOutputStream(new BufferedOutputStream(out))) {
			for(PathFinder minuteOfDay : pathFinder.iterateMinutesOfDay()) {
				Path minuteFilePath = minuteOfDay.getMinuteFilePath();
				try(InputStream in = Files.newInputStream(closeWriter(minuteFilePath));
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

	/**
	 * Close a writer which is currently writing into the given path. If there is no such writer nothing happens.
	 * 
	 * @param path The path for which a writer should be closed.
	 * @return The path which has been supplied for method chaining.
	 * @throws IOException If closing the writer fails.
	 */
	private Path closeWriter(Path path) throws IOException {
		ObjectNodeWriter writer = writers.remove(path);
		if(writer != null) {
			writer.close();
		}
		return path;
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
}