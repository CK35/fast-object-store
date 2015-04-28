package de.ck35.objectstore.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.objectstore.api.Bucket;
import de.ck35.objectstore.api.StoredObjectNode;
import de.ck35.objectstore.fs.BucketCommand.ListBucketsCommand;
import de.ck35.objectstore.fs.BucketCommand.ReadCommand;
import de.ck35.objectstore.fs.BucketCommand.WriteCommand;

public class BucketCommandProcessor implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger(BucketCommandProcessor.class);

	private final Path basePath;
	private final Function<BucketData, FilesystemBucket> pathBucketFactory;
	
	private final BlockingQueue<BucketCommand<?>> commands;

	public BucketCommandProcessor(Path basePath,
								  Function<ObjectNode, DateTime> dateTimeFunction,
	                        	  Function<BucketData, FilesystemBucket> pathBucketFactory, 
	                        	  BlockingQueue<BucketCommand<?>> commands) {
		this.basePath = basePath;
		this.pathBucketFactory = pathBucketFactory;
		this.commands = commands;
	}

	@Override
	public void run() {
		Context context = new Context();
		try {
			init(context);
			try {
				while(!Thread.interrupted()) {
					BucketCommand<?> command = commands.take();
					try {						
						runCommand(command, context);
					} catch(Exception e) {
						LOG.error("Error while working on command: '{}'!", command, e);
						command.setResult(null);
					}
				}
			} catch(InterruptedException e) {
				LOG.info("Command processor has been interrupted and will be closed now.");
			}
		} finally {
			close(context.getBuckets().values());
		}
	}
	
	public void init(Context context) {
		close(context.getBuckets().values());
		try {
			try(DirectoryStream<Path> stream = Files.newDirectoryStream(basePath, new Filter<Path>() {
				@Override
				public boolean accept(Path entry) throws IOException {
					return Files.isDirectory(entry);
				}
			})) {
				for(Path bucketPath : stream) {
					BucketData data = BucketData.load(bucketPath);
					context.getBuckets().put(data.getName(), pathBucketFactory.apply(data));
				}
			};
		} catch (IOException e) {
			throw new RuntimeException("Initializing context failed!", e); 
		}
	}
	
	public void runCommand(BucketCommand<?> command, Context context) {
		if(command instanceof WriteCommand) {
			command.setResult(runWriteCommand((WriteCommand) command, context));
			
		} else if(command instanceof ReadCommand) {
			runReadCommand((ReadCommand) command, context);
			command.commandCompleted();
			
		} else if(command instanceof ListBucketsCommand) {
			command.setResult(runListBucketsCommand((ListBucketsCommand) command, context));
			
		} else {
			throw new IllegalArgumentException("Unknown command!");
		}
	}
	
	public Iterable<Bucket> runListBucketsCommand(ListBucketsCommand command, Context context) {
		return new ArrayList<Bucket>(context.getBuckets().values());
	}
	
	public StoredObjectNode runWriteCommand(WriteCommand command, Context context) {
		FilesystemBucket bucket = context.getBuckets().get(command.getBucketName());
		if(bucket == null) {
			BucketData bucketData;
			try {
				bucketData = BucketData.create(basePath, command.getBucketName(), command.getBucketType());
			} catch(IOException e) {
				throw new RuntimeException("Creating new bucket: '" + command.getBucketName() + "' with type: '" + command.getBucketType() + "' failed!", e);
			}
			bucket = pathBucketFactory.apply(bucketData);
			context.getBuckets().put(command.getBucketName(), bucket);
		}
		try {
			return bucket.write(command.getNode());
		} catch (IOException e) {
			throw new RuntimeException("Writing object into bucket failed!", e);
		}
	}
	
	public void runReadCommand(ReadCommand command, Context context) {
		FilesystemBucket bucket = context.getBuckets().get(command.getBucketName());
		if(bucket == null) {
			return;
		}
		try {
			bucket.read(command.getInterval(), command.getCallable());
		} catch (IOException e) {
			throw new RuntimeException("Reading from bucket failed!", e);
		}
	}
	
	public static void close(Iterable<FilesystemBucket> buckets) {
		for(FilesystemBucket bucket : buckets) {
			try {				
				bucket.close();
			} catch(IOException e) {
				LOG.warn("Error while closing bucket: {}.", bucket.getBucketData(), e);
			}
		}
	}
	
	public static class Context {
		
		private Map<String, FilesystemBucket> buckets;
		
		public Context() {
			this.buckets = new HashMap<>();
		}
		public Map<String, FilesystemBucket> getBuckets() {
			return buckets;
		}
	}
}