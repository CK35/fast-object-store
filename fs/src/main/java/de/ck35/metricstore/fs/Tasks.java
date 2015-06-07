package de.ck35.metricstore.fs;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.ErrorHandler;

import com.google.common.base.Supplier;

import de.ck35.metricstore.api.MetricBucket;

public interface Tasks {

	@ManagedResource
	public static class TasksErrorHandler implements ErrorHandler {
		
		private static final Logger LOG = LoggerFactory.getLogger(Tasks.TasksErrorHandler.class);
		
		private final AtomicLong totalTaskErrors;
		
		public TasksErrorHandler() {
			this.totalTaskErrors = new AtomicLong();
		}
		@Override
		public void handleError(Throwable t) {
			this.totalTaskErrors.incrementAndGet();
			LOG.error("Task Errorhandler invoked!", t);
		}
		@ManagedAttribute
		public long getTotalTaskErrors() {
			return totalTaskErrors.get();
		}
	}
	
	@ManagedResource
	public static abstract class BaseTask implements Runnable {
		
		private final FilesystemMetricRepository repository;
		private final Supplier<Integer> numberOfDays;
		
		private final AtomicLong totalRunCount;
		private final AtomicReference<DateTime> lastRunRef;

		public BaseTask(FilesystemMetricRepository repository, Supplier<Integer> numberOfDays) {
			this.repository = repository;
			this.numberOfDays = numberOfDays;
			this.totalRunCount = new AtomicLong();
			this.lastRunRef = new AtomicReference<>();
		}
		@Override
		public void run() {
			this.lastRunRef.set(DateTime.now());
			this.totalRunCount.incrementAndGet();
			this.run(repository, date());
		}
		
		public LocalDate date() {
			return LocalDate.now().minusDays(numberOfDays.get());
		}
		
		public abstract void run(FilesystemMetricRepository repository, LocalDate date);
		
		@ManagedAttribute
		public String getLastRun() {
			DateTime lastRun = lastRunRef.get();
			return lastRun == null ? null : lastRun.toString();
		}
		@ManagedAttribute
		public long getTotalRunCount() {
			return totalRunCount.get();
		}
		@ManagedAttribute
		public String getDate() {
			return date().toString();
		}
	}
	
	@ManagedResource
	public static class CompressTask extends BaseTask {
		
		public CompressTask(FilesystemMetricRepository repository, Supplier<Integer> uncompressedDays) {
			super(repository, uncompressedDays);
		}
		public void run(FilesystemMetricRepository repository, LocalDate date) {
			for(MetricBucket bucket : repository.listBuckets()) {
				repository.compress(bucket, date);
			}
		}
	}
	
	@ManagedResource
	public static class DeleteTask extends BaseTask {
		
		public DeleteTask(FilesystemMetricRepository repository, Supplier<Integer> daysToKeep) {
			super(repository, daysToKeep);
		}
		public void run(FilesystemMetricRepository repository, LocalDate date) {
			for(MetricBucket bucket : repository.listBuckets()) {
				repository.delete(bucket, date);
			}
		}
	}
}