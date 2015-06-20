package de.ck35.metricstore.benchmark;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import de.ck35.metricstore.api.MetricRepository;

public class BenchmarkTest {

    private List<Entry<BucketInfo, ObjectNode>> testData;
    
    private MetricRepository repository;
    private Iterable<Entry<BucketInfo, ObjectNode>> testDataIterator;
    private Supplier<ExecutorService> executorServiceSupplier;
    private int threadCount;
    private int timeout;
    private TimeUnit unit;

    private ObjectNode node;
    
    public BenchmarkTest() {
        BucketInfo bucketInfo = new BucketInfo("a", "a-type");
        this.node = mock(ObjectNode.class);
        this.testData = ImmutableList.of(Maps.immutableEntry(bucketInfo, this.node));
        this.repository = mock(MetricRepository.class);
        this.testDataIterator = testData;
        this.executorServiceSupplier = Suppliers.ofInstance(Executors.newFixedThreadPool(1));
        this.threadCount = 1;
        this.timeout = 1;
        this.unit = TimeUnit.MINUTES;
    }
    
    public Benchmark benchmark() {
        return new Benchmark(repository, testDataIterator, executorServiceSupplier, threadCount, timeout, unit);
    }
    
    @Test
    public void test() {
        Benchmark benchmark = benchmark();
        benchmark.run();
        verify(repository).wirte("a", "a-type", node);
    }
    
    @Test(expected=RuntimeException.class)
    public void testInterrupt() throws InterruptedException {
        ExecutorService executorService = mock(ExecutorService.class);
        this.executorServiceSupplier = Suppliers.ofInstance(executorService);
        when(executorService.awaitTermination(timeout, unit)).thenThrow(new InterruptedException());
        
        Benchmark benchmark = benchmark();
        benchmark.run();
    }

}