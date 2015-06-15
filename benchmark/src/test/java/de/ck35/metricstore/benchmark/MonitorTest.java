package de.ck35.metricstore.benchmark;

import static org.junit.Assert.*;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

}