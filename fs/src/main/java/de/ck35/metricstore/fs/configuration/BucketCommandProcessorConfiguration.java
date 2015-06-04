package de.ck35.metricstore.fs.configuration;

import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.google.common.base.Function;

import de.ck35.metricstore.fs.BucketCommandProcessor;
import de.ck35.metricstore.fs.BucketCommandProcessorThread;
import de.ck35.metricstore.fs.BucketData;
import de.ck35.metricstore.fs.WritableFilesystemBucket;

/**
 * Configuration for the {@link BucketCommandProcessor}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@Configuration
public class BucketCommandProcessorConfiguration {

    @Autowired Environment env;
    @Autowired Function<BucketData, WritableFilesystemBucket> writableFilesystemBucketFactory;
    
    @Bean
    public BucketCommandProcessor bucketCommandProcessor() {
        return new BucketCommandProcessor(Paths.get(env.getRequiredProperty(PropPrefix.join("basepath"))), 
                                          writableFilesystemBucketFactory);
    }
    
    @Bean
    public BucketCommandProcessorThread managedBucketCommandProcessorThread() {
        return new BucketCommandProcessorThread();
    }
    
}