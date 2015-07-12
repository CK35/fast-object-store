package de.ck35.metricstore.fs.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import de.ck35.metricstore.util.configuration.ObjectMapperConfiguration;

@Configuration
@Import({ObjectMapperConfiguration.class,
         BucketCommandProcessorConfiguration.class,
         BucketCommandQueueConfiguration.class,
         FilesystemMetricRepositoryConfiguration.class,
         TasksConfiguration.class,
         WritableFilesystemBucketFactoryConfiguration.class})
public class RootConfiguration {}