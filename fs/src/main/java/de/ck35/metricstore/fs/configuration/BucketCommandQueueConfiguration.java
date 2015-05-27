package de.ck35.metricstore.fs.configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import de.ck35.metricstore.fs.BucketCommand;

@Configuration
public class BucketCommandQueueConfiguration {

    public static int DEFAULT_COMMAND_CAPACITY = 10_000;
    
    @Autowired Environment env;
    
    @Bean
    public BlockingQueue<BucketCommand<?>> bucketCommandQueue() {
        return new ArrayBlockingQueue<BucketCommand<?>>(env.getProperty(PropPrefix.join("commands.capacity"), 
                                                                        Integer.class, 
                                                                        DEFAULT_COMMAND_CAPACITY));
    }
    
}