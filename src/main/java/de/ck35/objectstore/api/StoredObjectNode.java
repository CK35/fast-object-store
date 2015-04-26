package de.ck35.objectstore.api;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface StoredObjectNode {

	Bucket getBucket();
	
	DateTime getTimestamp();
	
	ObjectNode getObjectNode();
	
}