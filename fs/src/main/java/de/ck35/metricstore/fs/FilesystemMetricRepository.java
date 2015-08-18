package de.ck35.metricstore.fs;

import java.util.Objects;

import org.joda.time.Interval;
import org.joda.time.LocalDate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;
import de.ck35.metricstore.fs.BucketCommand.CompressCommand;
import de.ck35.metricstore.fs.BucketCommand.DeleteCommand;
import de.ck35.metricstore.fs.BucketCommand.ListBucketsCommand;
import de.ck35.metricstore.fs.BucketCommand.ReadCommand;
import de.ck35.metricstore.fs.BucketCommand.WriteCommand;
import de.ck35.metricstore.util.DayBasedIntervalSplitter;
import de.ck35.metricstore.util.io.MetricsIOException;

/**
 * The filesystem based implementation of the {@link MetricRepository}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class FilesystemMetricRepository implements MetricRepository {

	private final Predicate<BucketCommand<?>> commands;
	private final Supplier<Integer> readBufferSizeSetting;
	
	public FilesystemMetricRepository(Predicate<BucketCommand<?>> commands,
	                                  Supplier<Integer> readBufferSizeSetting) {
		this.commands = Objects.requireNonNull(commands);
        this.readBufferSizeSetting = Objects.requireNonNull(readBufferSizeSetting);
	}
	
	public <T extends BucketCommand<?>> T appendCommand(T command) {
	    if(!commands.apply(command)) {
	        throw new MetricsIOException("Could not append next command: '" + command + "' into queue!", null);
	    }
		return command;
	}
	
	@Override
	public Iterable<MetricBucket> listBuckets() {
		return appendCommand(new ListBucketsCommand()).getResult();
	}

	@Override
	public StoredMetric write(String bucketName, String bucketType, ObjectNode node) {
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
	
	public void compress(MetricBucket bucket, LocalDate compressUntil) {
		appendCommand(new CompressCommand(bucket.getName(), compressUntil));
	}
	
	public void delete(MetricBucket bucket, LocalDate deleteUntil) {
		appendCommand(new DeleteCommand(bucket.getName(), deleteUntil));
	}
}