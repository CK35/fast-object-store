package de.ck35.objectstore.api;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface BucketRepository {

	Iterable<Bucket> listBuckets();
	
	void read(String bucketName, Interval interval, StoredObjectNodeCallable callable);
	
	StoredObjectNode wirte(String bucketName, String bucketType, ObjectNode node);
	
}