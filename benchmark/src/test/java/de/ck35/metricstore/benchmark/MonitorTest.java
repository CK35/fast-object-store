package de.ck35.metricstore.benchmark;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import de.ck35.metricstore.benchmark.Monitor.SystemState;
import de.ck35.metricstore.benchmark.configuration.JMXConfiguration;
import de.ck35.metricstore.benchmark.configuration.MonitorConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={JMXConfiguration.class, MonitorConfiguration.class})
public class MonitorTest {
    
    @Autowired Monitor monitor;
    
    @Test
    public void testConfiguration() throws InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        threadPool.submit(monitor);
        monitor.awaitRun();
        NavigableMap<DateTime,SystemState> result = monitor.awaitResult();
        assertFalse(result.isEmpty());
        Entry<DateTime, SystemState> entry = result.entrySet().iterator().next();
        assertTrue(entry.getValue().getCpuUsage().isPresent());
        assertTrue(entry.getValue().getHeapUsage().isPresent());
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testRun() throws InterruptedException {
        Supplier<Optional<Long>> totalProcessedCommandsSupplier = mock(Supplier.class);
        Supplier<Optional<Double>> cpuUsageSupplier = mock(Supplier.class);
        Supplier<Optional<Double>> heapUsageSupplier = mock(Supplier.class);

        when(totalProcessedCommandsSupplier.get()).thenReturn(Optional.of(1l));
        when(cpuUsageSupplier.get()).thenReturn(Optional.of(2d));
        when(heapUsageSupplier.get()).thenReturn(Optional.of(3d));
        
        Monitor monitor = new Monitor(totalProcessedCommandsSupplier, cpuUsageSupplier, heapUsageSupplier, 1, TimeUnit.MILLISECONDS);
        final UncaughtExceptionHandler handler = Thread.currentThread().getUncaughtExceptionHandler();
        ExecutorService executorService = Executors.newFixedThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setUncaughtExceptionHandler(handler);
                return thread;
            }
        });
        executorService.submit(monitor);
        
        verify(totalProcessedCommandsSupplier, timeout(10000).atLeast(5)).get();
        verify(cpuUsageSupplier, timeout(10000).atLeast(5)).get();
        verify(heapUsageSupplier, timeout(10000).atLeast(5)).get();
        
        NavigableMap<DateTime,SystemState> result = monitor.awaitResult();
        assertTrue(result.size() >= 5);
        
    }

}