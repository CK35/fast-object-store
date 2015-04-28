package de.ck35.objectstore.fs;

import java.util.concurrent.BlockingQueue;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.objectstore.api.Bucket;
import de.ck35.objectstore.api.BucketRepository;
import de.ck35.objectstore.api.StoredObjectNode;
import de.ck35.objectstore.api.StoredObjectNodeCallable;
import de.ck35.objectstore.fs.BucketCommand.ListBucketsCommand;
import de.ck35.objectstore.fs.BucketCommand.WriteCommand;

public class FilesystemBucketRepository implements BucketRepository {

	private final BlockingQueue<BucketCommand<?>> commands;
	
	public FilesystemBucketRepository(BlockingQueue<BucketCommand<?>> commands) {
		this.commands = commands;
	}
	
	public <T extends BucketCommand<?>> T appendCommand(T command) {
		try {
			commands.put(command);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while appending command: '" + command + "' into queue!", e);
		}
		return command;
	}
	
	@Override
	public Iterable<Bucket> listBuckets() {
		return appendCommand(new ListBucketsCommand()).getResult();
	}

	@Override
	public StoredObjectNode wirte(String bucketName, String bucketType, ObjectNode node) {
		return appendCommand(new WriteCommand(bucketName, bucketType, node)).getResult();
	}

	@Override
	public void read(String bucketName, Interval interval, StoredObjectNodeCallable callable) {
		
	}

}