package de.ck35.metricstore.fs.configuration;

import java.util.concurrent.BlockingQueue;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import de.ck35.metricstore.fs.BucketCommand;
import de.ck35.metricstore.fs.FilesystemMetricRepository;
import de.ck35.metricstore.util.MinimumIntSetting;

/**
 * Configuration for the {@link FilesystemMetricRepository}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@Configuration
public class FilesystemMetricRepositoryConfiguration {

    public static int DEFAULT_READ_BUFFER_SIZE = 100_000;
    
    @Autowired Environment env;
    @Resource(name="bucketCommandQueue") BlockingQueue<BucketCommand<?>> bucketCommandQueue;
    
    @Bean
    public MinimumIntSetting readBufferSizeSetting() {
        return new MinimumIntSetting(DEFAULT_READ_BUFFER_SIZE, 1, env.getProperty(PropPrefix.join("readbuffer.size"), Integer.class));
    }
    
    @Bean
    public FilesystemMetricRepository filesystemMetricRepository() {
        return new FilesystemMetricRepository(bucketCommandQueue, readBufferSizeSetting());
    }
    
}