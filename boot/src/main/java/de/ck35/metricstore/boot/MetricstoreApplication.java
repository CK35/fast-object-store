package de.ck35.metricstore.boot;

import org.springframework.boot.SpringApplication;

import de.ck35.metricstore.configuration.RootConfiguration;

public class MetricstoreApplication extends SpringApplication {

    public static void main(String[] args) {
        MetricstoreApplication.run(RootConfiguration.class, args);
    }
    
}