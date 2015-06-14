package de.ck35.metricstore.benchmark;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;

import de.ck35.metricstore.api.MetricRepository;

public class Benchmark implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);
    
	private final MetricRepository repository;
	private final Iterable<Entry<BucketInfo, ObjectNode>> dataIterable;

    private final int threadCount;
    private final int timeout;
    private final TimeUnit unit;
    
	public Benchmark(MetricRepository repository, 
	                 Iterable<Entry<BucketInfo, ObjectNode>> testDataIterator,
	                 int threadCount,
	                 int timeout,
	                 TimeUnit unit) {
        this.repository = repository;
        this.dataIterable = testDataIterator;
        this.threadCount = threadCount;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
	public void run() {
	    LOG.info("Starting benchmark.");
	    DataSupplier dataSupplier = new DataSupplier(dataIterable.iterator());
	    ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
	    try {
	        for(int thread = 0 ; thread < threadCount ; thread++) {
	            threadPool.submit(new MetricRepositoryWriter(repository, dataSupplier));
	        }
	        threadPool.shutdown();
	        threadPool.awaitTermination(timeout, unit);
	    } catch (InterruptedException e) {
	        throw new RuntimeException("Benchmark interrupted!");
        } finally {
	        threadPool.shutdownNow();
	    }
	    LOG.info("Benchmark done.");
	}
    
    public static class DataSupplier implements Supplier<Entry<BucketInfo, ObjectNode>> {
        
        private final Iterator<Entry<BucketInfo, ObjectNode>> iter;
        
        public DataSupplier(Iterator<Entry<BucketInfo, ObjectNode>> iter) {
            this.iter = iter;
        }
        @Override
        public synchronized Entry<BucketInfo, ObjectNode> get() {
            if(iter.hasNext()) {
                return iter.next();
            } else {                
                return null;
            }
        }
    }

    public static class MetricRepositoryWriter implements Runnable {
        
        private final MetricRepository repository;
        private final Supplier<Entry<BucketInfo, ObjectNode>> dataSupplier;

        public MetricRepositoryWriter(MetricRepository repository, Supplier<Entry<BucketInfo, ObjectNode>> dataSupplier) {
            this.repository = repository;
            this.dataSupplier = dataSupplier;
        }
        @Override
        public void run() {
            for(Entry<BucketInfo, ObjectNode> next = dataSupplier.get() ; next != null ; next = dataSupplier.get()) {
                repository.wirte(next.getKey().getBucketName(), next.getKey().getBucketType(), next.getValue());
            }
        }
    }
}