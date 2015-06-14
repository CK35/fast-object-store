package de.ck35.metricstore.benchmark;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import de.ck35.metricstore.fs.BucketCommandProcessor;

public class Monitor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);
    
	private final BucketCommandProcessor bucketCommandProcessor;
	
	private final AtomicBoolean enabled;
	private final CountDownLatch runLatch;
	private final CountDownLatch resultLatch;
	private final AtomicReference<NavigableMap<DateTime, SystemState>> resultReference;

    private final int pollTimeout;
    private final TimeUnit unit;
	
    public Monitor(BucketCommandProcessor bucketCommandProcessor, int pollTimeout, TimeUnit unit) {
        this.bucketCommandProcessor = bucketCommandProcessor;
        this.pollTimeout = pollTimeout;
        this.unit = unit;
        this.enabled = new AtomicBoolean(true);
        this.runLatch = new CountDownLatch(1);
        this.resultLatch = new CountDownLatch(1);
        this.resultReference = new AtomicReference<>();
	}
	
	@Override
	public void run() {
	    this.resultLatch.countDown();
	    NavigableMap<DateTime, SystemState> stateMap = new TreeMap<>();
	    try {
	        try {
	            Optional<Entry<DateTime, SystemState>> lastState = Optional.absent();
	            while(enabled.get()) {
	                DateTime now = now();
	                lastState = Optional.of(Maps.immutableEntry(now, systemState(lastState)));
	                stateMap.put(now, lastState.get().getValue());
	                Thread.sleep(TimeUnit.MILLISECONDS.convert(pollTimeout, unit));
	            }
	        } catch (InterruptedException e) {
	            LOG.warn("Monitor Thread interrupted.");
	        }
	    } finally {
	        resultReference.set(Collections.unmodifiableNavigableMap(stateMap));
	        resultLatch.countDown();
	    }
	}
	
	public void awaitRun() throws InterruptedException {
	    this.runLatch.await();
	}
	
	public NavigableMap<DateTime, SystemState> awaitResult() throws InterruptedException {
	    this.enabled.set(false);
	    this.resultLatch.await();
        return resultReference.get();
    }

    private SystemState systemState(Optional<Entry<DateTime, SystemState>> lastState) {
        long totalProcessedCommands = bucketCommandProcessor.getTotalProcessedCommands();
        
        return new SystemState(0d, 
                               0d,
                               totalProcessedCommands, 
                               0);
    }
	
	public DateTime now() {
	    return DateTime.now();
	}
	
	public static class SystemState {
	    
	    private final double cpuUsage;
	    private final double heapUsage;
	    private final long totalProcessedCommands;
	    private final long processedCommandsPerMinute;
	    
        public SystemState(double cpuUsage,
                           double heapUsage,
                           long totalProcessedCommands,
                           long processedCommandsPerMinute) {
            this.cpuUsage = cpuUsage;
            this.heapUsage = heapUsage;
            this.totalProcessedCommands = totalProcessedCommands;
            this.processedCommandsPerMinute = processedCommandsPerMinute;
        }
	    
	    
	}
}