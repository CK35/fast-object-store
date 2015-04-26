package de.ck35.objectstore.fs;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.objectstore.api.Bucket;
import de.ck35.objectstore.api.ObjectNodeStream;
import de.ck35.objectstore.api.StoredObjectNode;

public class BucketCommand<T> {

	private final CountDownLatch resultLatch;
	private final AtomicReference<T> resultReference;
	
	public BucketCommand() {
		this.resultLatch = new CountDownLatch(1);
		this.resultReference = new AtomicReference<>();
	}
	
	public T getResult() {
		try {			
			resultLatch.await();
		} catch(InterruptedException e) {
			throw new RuntimeException("Could not await result. Calling Thread has been interrupted.", e);
		}
		return resultReference.get();
	}
	
	public void setResult(T result) {
		resultReference.set(result);
		resultLatch.countDown();
	}
	
	public static class ListBucketsCommand extends BucketCommand<Iterable<Bucket>> {
		
		public ListBucketsCommand() {
			super();
		}
		@Override
		public String toString() {
			return "ListBucketsCommand";
		}
	}
	
	public static class WriteCommand extends BucketCommand<StoredObjectNode> {
		
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
	
	public static class ReadCommand extends BucketCommand<ObjectNodeStream> {
		
		private final String bucketName;
		private final Interval interval;
		
		public ReadCommand(String bucketName, Interval interval) {
			super();
			this.bucketName = bucketName;
			this.interval = interval;
		}
		
		public String getBucketName() {
			return bucketName;
		}
		public Interval getInterval() {
			return interval;
		}
		@Override
		public String toString() {
			return "ReadCommand [bucketName=" + bucketName + ", interval=" + interval + "]";
		}
	}
}