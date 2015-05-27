package de.ck35.metricstore.fs;

import static de.ck35.metricstore.util.TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.fs.configuration.ObjectMapperConfiguration;
import de.ck35.metricstore.util.LRUCache;
import de.ck35.metricstore.util.TimestampFunction;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={ObjectMapperConfiguration.class})
public class FilesystemBucketTest {

	private static final String BUCKET_TYPE = "TestBucketType";
	private static final String BUCKET_NAME = "TestBucket";

	private static final Logger LOG = LoggerFactory.getLogger(FilesystemBucketTest.class);
	
	@Autowired ObjectMapper mapper;
	
	private Function<ObjectNode, DateTime> timestampFunction;
	private Function<Path, ObjectNodeWriter> writerFactory;
	private Function<Path, ObjectNodeReader> readerFactory;
	private LRUCache<Path, ObjectNodeWriter> writers;
	
	private BucketData bucketData;

	@Before
	public void before() throws IOException {
		this.timestampFunction = new TimestampFunction();
		this.writerFactory = new ObjectNodeWriter.Factory(mapper.getFactory());
		this.readerFactory = new ObjectNodeReader.Factory(mapper);
		this.writers = new LRUCache<>(5);
		
		Path workdir = Files.createTempDirectory("FilesystemBucketTest");
		LOG.debug("Running test inside tmp work dir: '{}'.", workdir);
		this.bucketData = new BucketData(workdir, BUCKET_NAME, BUCKET_TYPE);
	}
	
	@After
	public void after() throws IOException {
		WritableFilesystemBucket.clearDirectory(bucketData.getBasePath());
		Files.delete(bucketData.getBasePath());
	}
	
	@Test
	public void testBucketData() throws IOException {
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			assertEquals(bucketData, bucket.getBucketData());
			assertEquals(BUCKET_NAME, bucket.getName());
			assertEquals(BUCKET_TYPE, bucket.getType());
		}
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testClose() throws IOException {
		ObjectNodeWriter writer1 = mock(ObjectNodeWriter.class);
		ObjectNodeWriter writer2 = mock(ObjectNodeWriter.class);
		doThrow(new IOException()).when(writer1).close();
		Iterator<ObjectNodeWriter> iterator = ImmutableList.of(writer1, writer2).iterator();
		this.writers = mock(LRUCache.class);
		when(writers.iterator()).thenReturn(iterator);
		try(WritableFilesystemBucket bucket = filesystemBucket()) {};
		verify(writer1).close();
		verify(writer2).close();
		assertEquals(0, writers.size());
	}
	
	@Test
	public void testReadInsideDayFileWithExplicitEnd() throws IOException, InterruptedException {
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			Path dayFile = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getDayFilePath();
			Files.createDirectories(dayFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(dayFile)) {
				writer.write(node(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toString(), "fieldA", "valueA1"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC).toString(), "fieldA", "valueA2"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 2, DateTimeZone.UTC).toString(), "fieldA", "valueA3"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 3, DateTimeZone.UTC).toString(), "fieldA", "valueA4"));
			}
			List<StoredMetric> result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC), 
			                                                             new DateTime(2015, 1, 1, 0, 3, DateTimeZone.UTC)), 2);
			
			assertEquals(new DateTime(2015,1,1,0,1, DateTimeZone.UTC), result.get(0).getTimestamp());
			assertEquals(new DateTime(2015,1,1,0,2, DateTimeZone.UTC), result.get(1).getTimestamp());
			assertEquals("valueA2", result.get(0).getObjectNode().path("fieldA").asText());
			assertEquals("valueA3", result.get(1).getObjectNode().path("fieldA").asText());
		}
	}
	
	@Test
	public void testReadInsideDayFile() throws IOException, InterruptedException {
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			Path dayFile = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getDayFilePath();
			Files.createDirectories(dayFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(dayFile)) {
				writer.write(node(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toString(), "fieldA", "valueA1"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 2, DateTimeZone.UTC).toString(), "fieldA", "valueA2"));
			}
			List<StoredMetric> result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), 
			                                                             new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC)), 1);
			
			assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(0).getTimestamp());
			assertEquals("valueA1", result.get(0).getObjectNode().path("fieldA").asText());
		}
	}
	
	@Test
	public void testRead() throws IOException, InterruptedException {
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			Path dayFile = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getDayFilePath();
			Files.createDirectories(dayFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(dayFile)) {
				writer.write(node(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toString(), "fieldA", "valueA1"));
				writer.write(node(new DateTime(2015, 1, 1, 23, 59, DateTimeZone.UTC).toString(), "fieldA", "valueA2"));
			}
			Path minuteFile = bucket.pathFinder(new DateTime(2015, 1, 2, 12, 0, DateTimeZone.UTC)).getMinuteFilePath();
			Files.createDirectories(minuteFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(minuteFile)) {
				writer.write(node(new DateTime(2015, 1, 2, 12, 0, DateTimeZone.UTC).toString(), "fieldA", "valueA3"));
			}
			List<StoredMetric> result = readMetrics(bucket, new Interval(new DateTime(2014, 12, 31, 23, 59, DateTimeZone.UTC), 
			                                                             new DateTime(2015, 1, 3, 0, 0, DateTimeZone.UTC)), 3);
			
			assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(0).getTimestamp());
			assertEquals(new DateTime(2015,1,1,23,59, DateTimeZone.UTC), result.get(1).getTimestamp());
			assertEquals(new DateTime(2015,1,2,12,0, DateTimeZone.UTC), result.get(2).getTimestamp());
			assertEquals("valueA1", result.get(0).getObjectNode().path("fieldA").asText());
			assertEquals("valueA2", result.get(1).getObjectNode().path("fieldA").asText());
			assertEquals("valueA3", result.get(2).getObjectNode().path("fieldA").asText());
		}
	}
	
	@Test
	public void testReadWithOpenedWrite() throws IOException, InterruptedException {
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			bucket.write(node(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toString(), "fieldA", "valueA1"));
			List<StoredMetric> result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), 
			                                                             new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC)), 1);
			assertEquals(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), result.get(0).getTimestamp());
			assertEquals("valueA1", result.get(0).getObjectNode().path("fieldA").asText());
		}
	}
	
	@Test
	public void testExpand() throws IOException, InterruptedException {
		Path minuteFile0;
		Path minuteFile1;
		Path minuteFile2;
		Path minuteFile3;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			minuteFile0 = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getMinuteFilePath();
			minuteFile1 = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC)).getMinuteFilePath();
			minuteFile2 = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 2, DateTimeZone.UTC)).getMinuteFilePath();
			minuteFile3 = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 3, DateTimeZone.UTC)).getMinuteFilePath();
			
			Path dayFile = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getDayFilePath();
			Files.createDirectories(dayFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(dayFile)) {
				writer.write(node(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toString(), "fieldA0", "valueA0"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).toString(), "fieldA0", "valueA1"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 2, DateTimeZone.UTC).toString(), "fieldA2", "valueA2"));
				writer.write(node(new DateTime(2015, 1, 1, 0, 3, DateTimeZone.UTC).toString(), "fieldA3", "valueA3"));
			}
			bucket.write(node(new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC).toString(), "fieldA1", "valueA1"));
		}
		
		assertNotEmptyFile(minuteFile0);
		assertNotEmptyFile(minuteFile1);
		assertNotEmptyFile(minuteFile2);
		assertNotEmptyFile(minuteFile3);
		
		List<StoredMetric> result;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), new DateTime(2015, 1, 1, 0, 4, DateTimeZone.UTC)), 5);
		}
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(0).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(1).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,1, DateTimeZone.UTC), result.get(2).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,2, DateTimeZone.UTC), result.get(3).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,3, DateTimeZone.UTC), result.get(4).getTimestamp());
		assertEquals("valueA0", result.get(0).getObjectNode().path("fieldA0").asText());
		assertEquals("valueA1", result.get(1).getObjectNode().path("fieldA0").asText());
		assertEquals("valueA1", result.get(2).getObjectNode().path("fieldA1").asText());
		assertEquals("valueA2", result.get(3).getObjectNode().path("fieldA2").asText());
		assertEquals("valueA3", result.get(4).getObjectNode().path("fieldA3").asText());
	}
	
	@Test
	public void testExpandWithExistingTMPFolder() throws IOException, InterruptedException {
		DateTime timestamp = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
		Path minuteFile;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			minuteFile = bucket.pathFinder(timestamp).getMinuteFilePath();
			Path dayFile = bucket.pathFinder(timestamp).getDayFilePath();
			Files.createDirectories(dayFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(dayFile)) {
				writer.write(node(timestamp.toString(), "fieldA", "valueA0"));
			}
			Path tmpDayFolder = bucket.pathFinder(timestamp).getTemporaryMinuteFilePath().getParent();
			Files.createDirectories(tmpDayFolder);
			Path tmpMinuteFile = bucket.pathFinder(timestamp).getTemporaryMinuteFilePath();
			try(ObjectNodeWriter writer = writerFactory.apply(tmpMinuteFile)) {
				writer.write(node(timestamp.toString(), "fieldA", "valueA0"));
			}
			bucket.write(node(timestamp.toString(), "fieldA", "valueA1"));
		}
		assertNotEmptyFile(minuteFile);
		List<StoredMetric> result;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			result = readMetrics(bucket, new Interval(timestamp, Period.minutes(1)), 2);
		}
		assertEquals(timestamp, result.get(0).getTimestamp());
		assertEquals("valueA0", result.get(0).getObjectNode().path("fieldA").asText());
		assertEquals(timestamp, result.get(1).getTimestamp());
		assertEquals("valueA1", result.get(1).getObjectNode().path("fieldA").asText());
	}
	
	@Test
	public void testAppend() throws IOException, InterruptedException {
		Path minuteFile1;
		Path minuteFile2;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {	
			minuteFile1 = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getMinuteFilePath();
			minuteFile2 = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 1, DateTimeZone.UTC)).getMinuteFilePath();
			bucket.write(node(new LocalDateTime(2015, 1, 1, 0, 0).toString(), "fieldA1", "valueA1"));
			bucket.write(node(new LocalDateTime(2015, 1, 1, 0, 1).toString(), "fieldB1", "valueB1"));
			bucket.write(node(new LocalDateTime(2015, 1, 1, 0, 0).toString(), "fieldA2", "valueA2"));
		}
		
		assertNotEmptyFile(minuteFile1);
		assertNotEmptyFile(minuteFile2);
		
		List<StoredMetric> result;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), new DateTime(2015, 1, 1, 0, 2, DateTimeZone.UTC)), 3);
		}
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(0).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(1).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,1, DateTimeZone.UTC), result.get(2).getTimestamp());
		assertEquals("valueA1", result.get(0).getObjectNode().path("fieldA1").asText());
		assertEquals("valueA2", result.get(1).getObjectNode().path("fieldA2").asText());
		assertEquals("valueB1", result.get(2).getObjectNode().path("fieldB1").asText());
	}
	
	@Test
	public void testAppendExisting() throws IOException, InterruptedException {
		try(WritableFilesystemBucket bucket = filesystemBucket()) {	
			Path minuteFile = bucket.pathFinder(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC)).getMinuteFilePath();
			Files.createDirectories(minuteFile.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(minuteFile)) {
				writer.write(node(new LocalDateTime(2015, 1, 1, 0, 0).toString(), "fieldA", "valueA1"));
			}
			bucket.write(node(new LocalDateTime(2015, 1, 1, 0, 0).toString(), "fieldA", "valueA2"));
		}
		
		List<StoredMetric> result;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), Period.minutes(1)), 2);
		}
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(0).getTimestamp());
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(1).getTimestamp());
		assertEquals("valueA1", result.get(0).getObjectNode().path("fieldA").asText());
		assertEquals("valueA2", result.get(1).getObjectNode().path("fieldA").asText());
	}
	
	@Test
	public void testCompress() throws IOException, InterruptedException {
		Path dayFileA;
		Path minuteFileB;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			DateTime timestampA = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
			DateTime timestampB = new DateTime(2015, 1, 2, 0, 0, DateTimeZone.UTC);
			dayFileA = bucket.pathFinder(timestampA).getDayFilePath();
			minuteFileB = bucket.pathFinder(timestampB).getMinuteFilePath();
			bucket.write(node(timestampA.toLocalDate().toString(), "fieldA", "valueA"));
			bucket.write(node(timestampB.toLocalDate().toString(), "fieldB", "valueB"));
			bucket.compressAll(new LocalDate(2015, 1, 2));
		}
		assertNotEmptyFile(dayFileA);
		assertNotEmptyFile(minuteFileB);
		
		List<StoredMetric> result;
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			result = readMetrics(bucket, new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), Period.days(3)), 2);
		}
		assertEquals(new DateTime(2015,1,1,0,0, DateTimeZone.UTC), result.get(0).getTimestamp());
		assertEquals(new DateTime(2015,1,2,0,0, DateTimeZone.UTC), result.get(1).getTimestamp());
		assertEquals("valueA", result.get(0).getObjectNode().path("fieldA").asText());
		assertEquals("valueB", result.get(1).getObjectNode().path("fieldB").asText());
	}
	
	@Test
	public void testDelete() throws IOException {
		DateTime timestampA = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
		DateTime timestampB = new DateTime(2015, 1, 2, 0, 0, DateTimeZone.UTC);
		DateTime timestampC = new DateTime(2015, 1, 3, 0, 0, DateTimeZone.UTC);
		
		Path dayFileA;
		Path dayFolderB;
		Path dayFolderC;
		
		try(WritableFilesystemBucket bucket = filesystemBucket()) {
			dayFileA = bucket.pathFinder(timestampA).getDayFilePath();
			dayFolderB = bucket.pathFinder(timestampB).getDayDirectoryPath();
			dayFolderC = bucket.pathFinder(timestampC).getDayDirectoryPath();
			Files.createDirectories(dayFileA.getParent());
			try(ObjectNodeWriter writer = writerFactory.apply(dayFileA)) {
				writer.write(node(timestampA.toString(), "fieldA", "valueA0"));
			}
			bucket.write(node(timestampB.toLocalDate().toString(), "fieldA", "valueA"));
			bucket.write(node(timestampC.toLocalDate().toString(), "fieldB", "valueB"));
			
			assertTrue(Files.isRegularFile(dayFileA));
			assertTrue(Files.isDirectory(dayFolderB));
			assertTrue(Files.isDirectory(dayFolderC));
			
			bucket.deletAll(timestampC.toLocalDate());
			
			assertFalse(Files.isRegularFile(dayFileA));
			assertFalse(Files.isDirectory(dayFolderB));
			assertTrue(Files.isDirectory(dayFolderC));
		}
	}
	
	@Test
	public void testClearDirectory() throws IOException {
		Path directory = Files.createTempDirectory("clearTest");
		LOG.debug("Using dir: '{}' for clear test.", directory);
		Path file1 = Files.createFile(directory.resolve("file1"));
		Path subDir = Files.createDirectory(directory.resolve("dir2"));
		Path file2 = Files.createFile(subDir.resolve("file2"));
		WritableFilesystemBucket.clearDirectory(directory);
		assertTrue(Files.isDirectory(directory));
		assertFalse(Files.isRegularFile(file1));
		assertFalse(Files.isDirectory(subDir));
		assertFalse(Files.isRegularFile(file2));
		Files.delete(directory);
	}
	
	public WritableFilesystemBucket filesystemBucket() {
		return new WritableFilesystemBucket(bucketData, timestampFunction, writerFactory, readerFactory, writers);
	}
	
	public ObjectNode node(String timestamp, String dataFieldName, String data) {
		ObjectNode result = mapper.getNodeFactory().objectNode();
		result.put(DEFAULT_TIMESTAMP_FILED_NAME, timestamp);
		result.put(dataFieldName, data);
		return result;
	}
	
	@SuppressWarnings("unchecked")
	public static List<StoredMetric> readMetrics(WritableFilesystemBucket bucket, Interval interval, int expectedMetrics) throws InterruptedException {
		Predicate<StoredMetric> predicate = mock(Predicate.class);
		when(predicate.apply(any(StoredMetric.class))).thenReturn(true);
		bucket.read(interval, predicate);
		ArgumentCaptor<StoredMetric> metricsCaptor = ArgumentCaptor.forClass(StoredMetric.class);
		verify(predicate, times(expectedMetrics)).apply(metricsCaptor.capture());
		return metricsCaptor.getAllValues();
	}

	public static void assertNotEmptyFile(Path path) throws IOException {
		assertTrue(Files.isRegularFile(path));
		assertTrue(Files.size(path) > 0);
	}
}