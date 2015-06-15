package de.ck35.metricstore.benchmark.configuration;

import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import de.ck35.metricstore.benchmark.MBeanAttributeSupplier;
import de.ck35.metricstore.benchmark.Monitor;
import de.ck35.metricstore.benchmark.MBeanAttributeSupplier.HeapMemoryUsageSupplier;

@Configuration
public class MonitorConfiguration {
    
    @Autowired Environment env;
    @Autowired MBeanServer mbeanServer;
    
    @Bean
    public Monitor monitor() throws MalformedObjectNameException {
        return new Monitor(new MBeanAttributeSupplier<>(mbeanServer, ObjectName.getInstance("de.ck35.metricstore.fs:type=BucketCommandProcessor,name=bucketCommandProcessor"), "TotalProcessedCommands", Long.class),
                           new MBeanAttributeSupplier<>(mbeanServer, ObjectName.getInstance("java.lang:type=OperatingSystem"), "ProcessCpuLoad", Double.class),
                           new HeapMemoryUsageSupplier(new MBeanAttributeSupplier<>(mbeanServer, ObjectName.getInstance("java.lang:type=Memory"), "HeapMemoryUsage", CompositeData.class)),
                           env.getProperty("metricstore.benchmark.monitor.pollTimeout", Integer.class, 10), 
                           env.getProperty("metricstore.benchmark.monitor.pollTimeout.unit", TimeUnit.class, TimeUnit.SECONDS));
    }
    
}