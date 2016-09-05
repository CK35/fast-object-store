package de.ck35.metricstore.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collections;

import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Suppliers;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.fs.Tasks.CompressTask;
import de.ck35.metricstore.fs.Tasks.DeleteTask;
import de.ck35.metricstore.fs.Tasks.TasksErrorHandler;

public class TasksTest {

	private FilesystemMetricRepository repository;
	private MetricBucket metricBucket;
	
	@Before
	public void before() {
		this.repository = mock(FilesystemMetricRepository.class);
		this.metricBucket = mock(MetricBucket.class);
		Iterable<MetricBucket> bucketIter = Collections.singleton(metricBucket);
		when(repository.listBuckets()).thenReturn(bucketIter);
	}
	
	@Test
	public void testTasksErrorHandler() {
		TasksErrorHandler errorHandler = new TasksErrorHandler();
		assertEquals(0, errorHandler.getTotalTaskErrors());
		errorHandler.handleError(new NullPointerException());
		assertEquals(1, errorHandler.getTotalTaskErrors());
	}
	
	@Test
	public void testCompressTask() {
		CompressTask task = new CompressTask(repository, Suppliers.ofInstance(2));
		assertEquals(0, task.getTotalRunCount());
		assertNull(task.getLastRun());
		task.run();
		LocalDate expected = LocalDate.now().minusDays(2);
		verify(repository).compress(metricBucket, expected);
		assertEquals(expected, task.date());
		assertEquals(expected.toString(), task.getDate());
		assertEquals(1, task.getTotalRunCount());
		assertNotNull(task.getLastRun());
	}
	
	@Test
	public void testDeleteTask() {
		DeleteTask task = new DeleteTask(repository, Suppliers.ofInstance(2));
		assertEquals(0, task.getTotalRunCount());
		assertNull(task.getLastRun());
		task.run();
		LocalDate expected = LocalDate.now().minusDays(2);
		verify(repository).delete(metricBucket, expected);
		assertEquals(expected, task.date());
		assertEquals(expected.toString(), task.getDate());
		assertEquals(1, task.getTotalRunCount());
		assertNotNull(task.getLastRun());
	}
}