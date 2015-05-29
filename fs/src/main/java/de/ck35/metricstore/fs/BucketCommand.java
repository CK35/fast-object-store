package de.ck35.metricstore.fs;

import java.util.Observable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;

/**
 * Holds all commands which can be processed by the {@link BucketCommandProcessor}.
 * 
 * @param <T> The result type of a command.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class BucketCommand<T> extends Observable {

	private static final Logger LOG = LoggerFactory.getLogger(BucketCommand.class);
	
	private final CountDownLatch resultLatch;
	private final AtomicBoolean completed;
	private final AtomicReference<T> resultReference;
	
	public BucketCommand() {
		this.resultLatch = new CountDownLatch(1);
		this.resultReference = new AtomicReference<>();
		this.completed = new AtomicBoolean();
	}
	
	public T getResult() {
		await();
		return resultReference.get();
	}
	
	public void await() {
		try {			
			resultLatch.await();
		} catch(InterruptedException e) {
			throw new RuntimeException("Could not await result. Calling Thread has been interrupted.", e);
		}
	}
	
	public void commandCompleted() {
		resultLatch.countDown();
		completed.set(true);
		setChanged();
		try {			
			notifyObservers();
		} catch(Exception e) {
			LOG.error("Error while notifying observers!", e);
		}
	}
	
	public boolean isCompleted() {
	    return completed.get();
	}
	
	@SuppressWarnings("unchecked")
	public void setResult(Object result) {
		resultReference.set((T)result);
	}
	
	public static class ListBucketsCommand extends BucketCommand<Iterable<MetricBucket>> {
		
		public ListBucketsCommand() {
			super();
		}
		@Override
		public String toString() {
			return "ListBucketsCommand";
		}
	}
	
	public static class WriteCommand extends BucketCommand<StoredMetric> {
		
		private final String bucketName;
		private final String bucketType;
		private final ObjectNode node;
		
		public WriteCommand(String bucketName, String bucketType, ObjectNode node) {
			this.bucketName = bucketName;
			this.bucketType = bucketType;
			this.node = node;
		}
		public String getBucketName() {
			return bucketName;
		}
		public String getBucketType() {
			return bucketType;
		}
		public ObjectNode getNode() {
			return node;
		}
		@Override
		public String toString() {
			return "WriteCommand [bucketName=" + bucketName + ", bucketType=" + bucketType + "]";
		}
	}
	
	public static class ReadCommand extends BucketCommand<Void> {
		
		private final String bucketName;
		private final Interval interval;
		private final Predicate<StoredMetric> predicate;
		
		public ReadCommand(String bucketName, Interval interval, Predicate<StoredMetric> predicate) {
			this.bucketName = bucketName;
			this.interval = interval;
			this.predicate = predicate;
		}
		
		public String getBucketName() {
			return bucketName;
		}
		public Interval getInterval() {
			return interval;
		}
		public Predicate<StoredMetric> getPredicate() {
			return predicate;
		}
		@Override
		public String toString() {
			return "ReadCommand [bucketName=" + bucketName + ", interval=" + interval + "]";
		}
	}
	
	public static class CompressCommand extends BucketCommand<Void> {
		
		private final String bucketName;
		private final LocalDate compressUntil;

		public CompressCommand(String bucketName, LocalDate compressUntil) {
			this.bucketName = bucketName;
			this.compressUntil = compressUntil;
		}
		public String getBucketName() {
			return bucketName;
		}
		public LocalDate getCompressUntil() {
			return compressUntil;
		}
		@Override
		public String toString() {
			return "CompressCommand [compressUntil=" + compressUntil + "]";
		}
	}
	
	public static class DeleteCommand extends BucketCommand<Void> {
		
		private final String bucketName;
		private final LocalDate deleteUntil;

		public DeleteCommand(String bucketName, LocalDate deleteUntil) {
			this.bucketName = bucketName;
			this.deleteUntil = deleteUntil;
		}
		public String getBucketName() {
			return bucketName;
		}
		public LocalDate getDeleteUntil() {
			return deleteUntil;
		}
		@Override
		public String toString() {
			return "DeleteCommand [bucketName=" + bucketName + ", deleteUntil="
					+ deleteUntil + "]";
		}
	}
}