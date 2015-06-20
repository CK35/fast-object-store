package de.ck35.metricstore.benchmark.configuration;

import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.io.Files;

import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.benchmark.Benchmark;
import de.ck35.metricstore.benchmark.Monitor;
import de.ck35.metricstore.benchmark.ReadVerification;
import de.ck35.metricstore.benchmark.Reporter;

@Configuration
@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan("de.ck35.metricstore.benchmark.configuration")
@ContextConfiguration(classes=BenchmarkConfigurationTest.class)
public class BenchmarkConfigurationTest {

    @Autowired Monitor monitor;
    @Autowired Benchmark benchmark;
    @Autowired ReadVerification readVerification;
    @Autowired Reporter reporter;
    @Autowired MetricRepository metricRepository;
    
    @BeforeClass
    public static void beforeContext() {
        System.setProperty("metricstore.fs.basepath", Files.createTempDir().getAbsolutePath());
    }
    
    @Test
    public void testBenchmark() {
        assertNotNull(monitor);
        assertNotNull(benchmark);
        assertNotNull(readVerification);
        assertNotNull(reporter);
        assertNotNull(metricRepository);
    }

}