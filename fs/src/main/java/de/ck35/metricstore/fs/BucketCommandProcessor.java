package de.ck35.metricstore.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.google.common.base.Function;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.fs.BucketCommand.CompressCommand;
import de.ck35.metricstore.fs.BucketCommand.DeleteCommand;
import de.ck35.metricstore.fs.BucketCommand.ListBucketsCommand;
import de.ck35.metricstore.fs.BucketCommand.ReadCommand;
import de.ck35.metricstore.fs.BucketCommand.WriteCommand;
import de.ck35.metricstore.util.io.MetricsIOException;

/**
 * Responsible for processing commands.
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
@ManagedResource
public class BucketCommandProcessor {
	
	private static final Logger LOG = LoggerFactory.getLogger(BucketCommandProcessor.class);

	private final Path basePath;
	private final Function<BucketData, WritableFilesystemBucket> pathBucketFactory;
	
	private final AtomicLong totalProcessedCommands;
    private final AtomicLong totalProcessedWriteCommands;
    private final AtomicLong totalProcessedReadCommands;
    private final AtomicLong totalProcessedListBucketCommands;
    private final AtomicLong totalProcessedCompressCommands;
    private final AtomicLong totalProcessedDeleteCommands;
    private final AtomicLong totalUnknownCommands;
    private final AtomicLong totalFailedCommands;
    private final AtomicReference<String> runningCommand;

	public BucketCommandProcessor(Path basePath,
	                        	  Function<BucketData, WritableFilesystemBucket> pathBucketFactory) {
		this.basePath = basePath;
		this.pathBucketFactory = pathBucketFactory;
		this.totalProcessedCommands = new AtomicLong();
		this.totalProcessedWriteCommands = new AtomicLong();
		this.totalProcessedReadCommands = new AtomicLong();
		this.totalProcessedListBucketCommands = new AtomicLong();
		this.totalProcessedCompressCommands = new AtomicLong();
		this.totalProcessedDeleteCommands = new AtomicLong();
		this.totalUnknownCommands = new AtomicLong();
		this.totalFailedCommands = new AtomicLong();
		this.runningCommand = new AtomicReference<>();
	}

	public void init(Context context) {
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
			BucketCommandProcessorThread.initialized();
		} catch (IOException e) {
			throw new MetricsIOException("Initializing context failed!", e); 
		}
	}
	
	public void close(Context context) {
        for(WritableFilesystemBucket bucket : context.getBuckets().values()) {
            try {               
                bucket.close();
            } catch(IOException e) {
                LOG.warn("Error while closing bucket: {}.", bucket.getBucketData(), e);
            }
        }
    }
	
	public void runCommand(BucketCommand<?> command, Context context) {
	    try {	        
	        runningCommand.set(command.toString());
	        totalProcessedCommands.incrementAndGet();
	        if(command instanceof WriteCommand) {
	            totalProcessedWriteCommands.incrementAndGet();
	            command.setResult(runWriteCommand((WriteCommand) command, context));
	            
	        } else if(command instanceof ReadCommand) {
	            totalProcessedReadCommands.incrementAndGet();
	            runReadCommand((ReadCommand) command, context);
	            
	        } else if(command instanceof ListBucketsCommand) {
	            totalProcessedListBucketCommands.incrementAndGet();
	            command.setResult(runListBucketsCommand((ListBucketsCommand) command, context));
	            
	        } else if(command instanceof CompressCommand) {
	            totalProcessedCompressCommands.incrementAndGet();
	            runCompressCommand((CompressCommand) command, context);
	            
	        } else if(command instanceof DeleteCommand) {
	            totalProcessedDeleteCommands.incrementAndGet();
	            runDeleteCommand((DeleteCommand) command, context);
	            
	        } else {
	            totalUnknownCommands.incrementAndGet();
	            throw new IllegalArgumentException("Unknown command!");
	        }
	    } catch(Exception e) {
            LOG.error("Error while working on command: '{}'!", command, e);
            totalFailedCommands.incrementAndGet();
	    } finally {
	        command.commandCompleted();
            runningCommand.set(null);
	    }
	}
	
	public Iterable<MetricBucket> runListBucketsCommand(ListBucketsCommand command, Context context) {
		return new ArrayList<MetricBucket>(context.getBuckets().values());
	}
	
	public StoredMetric runWriteCommand(WriteCommand command, Context context) {
		WritableFilesystemBucket bucket = context.getBuckets().get(command.getBucketName());
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
		return bucket.write(command.getNode());
	}
	
	public void runReadCommand(ReadCommand command, Context context) {
		WritableFilesystemBucket bucket = context.getBuckets().get(command.getBucketName());
		if(bucket == null) {
			return;
		}
		try {
			bucket.read(command.getInterval(), command.getPredicate());
		} catch (InterruptedException e) {
			LOG.warn("Interrupted while reading: '" + command + "'.");
		}
	}
	
	public void runCompressCommand(CompressCommand command, Context context) {
		WritableFilesystemBucket bucket = context.getBuckets().get(command.getBucketName());
		if(bucket == null) {
			return;
		}
		bucket.compressAll(command.getCompressUntil());
	}
	
	public void runDeleteCommand(DeleteCommand command, Context context) {
		WritableFilesystemBucket bucket = context.getBuckets().get(command.getBucketName());
		if(bucket == null) {
			return;
		}
		bucket.deletAll(command.getDeleteUntil());
	}
	
	public static class Context {
		
		private Map<String, WritableFilesystemBucket> buckets;
		
		public Context() {
			this.buckets = new HashMap<>();
		}
		public Map<String, WritableFilesystemBucket> getBuckets() {
			return buckets;
		}
	}

	@ManagedAttribute
    public long getTotalProcessedCommands() {
        return totalProcessedCommands.get();
    }
	@ManagedAttribute
    public long getTotalProcessedWriteCommands() {
        return totalProcessedWriteCommands.get();
    }
	@ManagedAttribute
    public long getTotalProcessedReadCommands() {
        return totalProcessedReadCommands.get();
    }
	@ManagedAttribute
    public long getTotalProcessedListBucketCommands() {
        return totalProcessedListBucketCommands.get();
    }
	@ManagedAttribute
    public long getTotalProcessedCompressCommands() {
        return totalProcessedCompressCommands.get();
    }
	@ManagedAttribute
    public long getTotalProcessedDeleteCommands() {
        return totalProcessedDeleteCommands.get();
    }
	@ManagedAttribute
    public long getTotalUnknownCommands() {
        return totalUnknownCommands.get();
    }
	@ManagedAttribute
    public long getTotalFailedCommands() {
        return totalFailedCommands.get();
    }
	@ManagedAttribute
	public String getRunningCommand() {
        return runningCommand.get();
    }
}