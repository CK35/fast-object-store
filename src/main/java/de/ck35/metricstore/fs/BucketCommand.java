package de.ck35.metricstore.fs;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.Interval;
import org.joda.time.LocalDate;

import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;

public class BucketCommand<T> {

	private final CountDownLatch resultLatch;
	private final AtomicReference<T> resultReference;
	
	public BucketCommand() {
		this.resultLatch = new CountDownLatch(1);
		this.resultReference = new AtomicReference<>();
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
	}
	
	@SuppressWarnings("unchecked")
	public void setResult(Object result) {
		resultReference.set((T)result);
		commandCompleted();
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
		private final StoredMetricCallable callable;
		
		public ReadCommand(String bucketName, Interval interval, StoredMetricCallable callable) {
			this.bucketName = bucketName;
			this.interval = interval;
			this.callable = callable;
		}
		
		public String getBucketName() {
			return bucketName;
		}
		public Interval getInterval() {
			return interval;
		}
		public StoredMetricCallable getCallable() {
			return callable;
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
}