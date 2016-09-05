package de.ck35.metricstore.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ObjectMapperConfiguration.class,
         WritableFilesystemBucketFactoryConfiguration.class,
         BucketCommandProcessorConfiguration.class,
         BucketCommandQueueConfiguration.class,
         FilesystemMetricRepositoryConfiguration.class,
         BucketMetricCacheRepositoryConfiguration.class,
         TasksConfiguration.class})
public class RootConfiguration {}