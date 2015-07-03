package de.ck35.metricstore.fs.configuration;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages={"de.ck35.metricstore.fs.configuration",
                             "de.ck35.metricstore.util.configuration"})
public class RootConfiguration {

}