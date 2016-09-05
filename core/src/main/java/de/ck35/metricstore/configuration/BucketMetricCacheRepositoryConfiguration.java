package de.ck35.metricstore.configuration;

import java.io.InputStream;
import java.io.OutputStream;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.MetricRepository;
import de.ck35.metricstore.cache.core.BucketMetricCache;
import de.ck35.metricstore.cache.core.CachePeriodWorker;
import de.ck35.metricstore.cache.core.CacheablePredicate;
import de.ck35.metricstore.cache.core.UTCCacheIntervalSupplier;
import de.ck35.metricstore.cache.core.buckets.BucketManager;
import de.ck35.metricstore.cache.core.buckets.ExpandedBucketManager;
import de.ck35.metricstore.util.MinimumIntSetting;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

@Configuration
public class BucketMetricCacheRepositoryConfiguration implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired MetricRepository metricRepository;
    @Autowired ObjectMapper mapper;
    @Autowired Environment env;
    
    @Bean
    public BucketMetricCache bucketMetricCacheRepository() {
        return new BucketMetricCache(metricRepository, bucketManager(), cacheablePredicate());
    }
    
    @Bean
    public CacheablePredicate cacheablePredicate() {
        return new CacheablePredicate(cacheIntervalSupplier());
    }
    
    @Bean
    public Supplier<Interval> cacheIntervalSupplier() {
        return new UTCCacheIntervalSupplier(Period.parse(env.getProperty("metricstore.cache.core.cachePeriod", "PT28h")), clock());
    }

    @Bean
    public Supplier<DateTime> clock() {
        return new UTCCacheIntervalSupplier.Clock();
    }
    
    @Bean
    public CachePeriodWorker cachePeriodWorker() {
        return new CachePeriodWorker(cacheIntervalSupplier(), clock(), Period.parse(env.getProperty("metricstore.cache.core.cacheCleanupPeriod", "PT1m")), bucketMetricCacheRepository(), bucketManager());
    }
    
    @Bean(destroyMethod="interrupt")
    public Thread cachePeriodWorkerThread() {
        return new Thread(cachePeriodWorker(), "CachePeriodWorkerThread");
    }
    
    @Bean
    public BucketManager bucketManager() {
        return new BucketManager(expandedBucketManagerFactory(), streamReaderFactory(), streamWriterFactory());
    }
    
    @Bean
    public Function<MetricBucket, ExpandedBucketManager> expandedBucketManagerFactory() {
        return new ExpandedBucketManager.ExpandedBucketManagerFactory(maxExpandedBucketsSetting());
    }

    @Bean
    public Supplier<Integer> maxExpandedBucketsSetting() {
        return new MinimumIntSetting(5, 1, env.getProperty("metricstore.cache.core.maxExpandedBuckets", Integer.TYPE));
    }

    @Bean
    public Function<InputStream, ObjectNodeReader> streamReaderFactory() {
        return new ObjectNodeReader.StreamFactory(mapper, Charsets.UTF_8);
    }

    @Bean
    public Function<OutputStream, ObjectNodeWriter> streamWriterFactory() {
        return new ObjectNodeWriter.StreamFactory(mapper.getFactory(), Charsets.UTF_8);
    }
    
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        cachePeriodWorkerThread().start();
    }
    
}