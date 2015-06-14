package de.ck35.metricstore.benchmark;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import de.ck35.metricstore.benchmark.Monitor.SystemState;


public class Reporter implements Callable<Void> {

	private final Monitor monitor;
	
    public Reporter(Monitor monitor) {
        this.monitor = monitor;
	}
	
    @Override
    public Void call() throws InterruptedException {
    	NavigableMap<DateTime, SystemState> result = monitor.awaitResult();
    	for(Entry<DateTime, SystemState> entry : result.entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue().getProcessedCommandsPerSecond().or(0d));
		}
    	return null;
    }
	
}