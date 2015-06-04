package de.ck35.metricstore.fs.configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.google.common.base.Predicate;

import de.ck35.metricstore.fs.ABQCommandQueue;
import de.ck35.metricstore.fs.BucketCommand;
import de.ck35.metricstore.fs.BucketCommandProcessor;
import de.ck35.metricstore.fs.BucketCommandProcessorThread;
import de.ck35.metricstore.fs.DisruptorCommandQueue;

/**
 * Configuration for the shared command queue. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@Configuration
public class BucketCommandQueueConfiguration {

    public enum QueueMode {
        ABQ,
        DISRUPTOR
    }
    public enum DisruptorWaitStrategy {
        BlockingWaitStrategy,
        BusySpinWaitStrategy,
        LiteBlockingWaitStrategy,
        PhasedBackoffWaitStrategy,
        SleepingWaitStrategy,
        TimeoutBlockingWaitStrategy,
        YieldingWaitStrategy
        
    }
    
    public static int DEFAULT_COMMAND_CAPACITY = 10_000;
    public static QueueMode DEFAULT_QUEUE_MODE = QueueMode.ABQ;
    public static DisruptorWaitStrategy DEFAULT_WAIT_STRATEGY = DisruptorWaitStrategy.BlockingWaitStrategy;
    
    @Autowired Environment env;
    @Autowired BucketCommandProcessor bucketCommandProcessor;
    @Autowired BucketCommandProcessorThread bucketCommandProcessorThread;
    
    @Bean
    public Predicate<BucketCommand<?>> bucketCommandQueue() throws Throwable {
        if(QueueMode.DISRUPTOR == getQueueMode()) {
            ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    bucketCommandProcessorThread.setTargetRunnableRef(runnable);
                    return bucketCommandProcessorThread;
                }
            });
            DisruptorCommandQueue commandQueue = DisruptorCommandQueue.build(getQueueSize(), 
                                                                             getDisruptorWaitStrategy(), 
                                                                             executor, 
                                                                             bucketCommandProcessor, 
                                                                             env, 
                                                                             PropPrefix.defaultPrefix().withPrefix("disruptor.waitStrategy."));
            commandQueue.start();
            bucketCommandProcessorThread.awaitInitialization();
            return commandQueue;
        } else {
            ABQCommandQueue commandQueue = new ABQCommandQueue(getQueueSize(), bucketCommandProcessor);
            bucketCommandProcessorThread.setTargetRunnableRef(commandQueue);
            bucketCommandProcessorThread.start();
            bucketCommandProcessorThread.awaitInitialization();
            return commandQueue;
                    
        }
    }
    
    public QueueMode getQueueMode() {
        return env.getProperty(PropPrefix.join("commands.queue.mode"), QueueMode.class, DEFAULT_QUEUE_MODE);
    }
    public int getQueueSize() {
        return env.getProperty(PropPrefix.join("commands.queue.capacity"), Integer.class, DEFAULT_COMMAND_CAPACITY);
    }
    public DisruptorWaitStrategy getDisruptorWaitStrategy() {
        return env.getProperty(PropPrefix.join("commands.queue.disruptor.waitStrategy"), DisruptorWaitStrategy.class, DEFAULT_WAIT_STRATEGY);
    }
    
}