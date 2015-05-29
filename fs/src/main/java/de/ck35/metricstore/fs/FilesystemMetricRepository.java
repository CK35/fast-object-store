package de.ck35.metricstore.fs;

import java.util.concurrent.BlockingQueue;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;
import de.ck35.metricstore.fs.BucketCommand.ListBucketsCommand;
import de.ck35.metricstore.fs.BucketCommand.ReadCommand;
import de.ck35.metricstore.fs.BucketCommand.WriteCommand;
import de.ck35.metricstore.util.DayBasedIntervalSplitter;

/**
 * The filesystem based implementation of the {@link MetricRepository}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class FilesystemMetricRepository implements MetricRepository {

	private final BlockingQueue<BucketCommand<?>> commands;
	private final Supplier<Integer> readBufferSizeSetting;
	
	public FilesystemMetricRepository(BlockingQueue<BucketCommand<?>> commands,
	                                  Supplier<Integer> readBufferSizeSetting) {
		this.commands = commands;
        this.readBufferSizeSetting = readBufferSizeSetting;
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
	public Iterable<MetricBucket> listBuckets() {
		return appendCommand(new ListBucketsCommand()).getResult();
	}

	@Override
	public StoredMetric wirte(String bucketName, String bucketType, ObjectNode node) {
		return appendCommand(new WriteCommand(bucketName, bucketType, node)).getResult();
	}

	@Override
	public void read(String bucketName, Interval interval, StoredMetricCallable callable) {
	    for(Interval subInterval : new DayBasedIntervalSplitter(interval)) {
	        try(StoredMetricReadCache readCache = new StoredMetricReadCache(readBufferSizeSetting)) {
	            ReadCommand command = new ReadCommand(bucketName, subInterval, readCache);
	            command.addObserver(readCache);
	            appendCommand(command);
	            while(readCache.hasNext()) {
	                callable.call(readCache.next());
	            }
	        }
	    }
	}
}