package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import de.ck35.metricstore.fs.BucketCommandProcessor.Context;
import de.ck35.metricstore.fs.configuration.BucketCommandQueueConfiguration.DisruptorWaitStrategy;

public class DisruptorCommandQueue implements Predicate<BucketCommand<?>>, Closeable {
    
    private final AtomicBoolean closed;
    private final Disruptor<BucketCommandEvent> disruptor;

    public DisruptorCommandQueue(int ringBufferSize, 
                                 WaitStrategy waitStrategy,
                                 Executor executor,
                                 BucketCommandProcessor commandProcessor) {
        disruptor = new Disruptor<DisruptorCommandQueue.BucketCommandEvent>(new BucketCommandEventFactory(), 
                                                                            ringBufferSize, 
                                                                            executor, 
                                                                            ProducerType.MULTI, 
                                                                            waitStrategy);
        disruptor.handleEventsWithWorkerPool(new BucketCommandWorkHandler[]{new BucketCommandWorkHandler(commandProcessor)});
        this.closed = new AtomicBoolean();
    }
    
    @Override
    public boolean apply(BucketCommand<?> input) {
        if(closed.get()) {
            return false;
        }
        disruptor.publishEvent(new BucketCommandEventTranslator(input));
        return true;
    }
    
    public void start() {
        this.disruptor.start();
    }
    
    @Override
    public void close() {
        this.closed.set(true);
        this.disruptor.shutdown();
    }

    public static DisruptorCommandQueue build(int ringBufferSize, 
                                              DisruptorWaitStrategy waitStrategy, 
                                              Executor executor,
                                              BucketCommandProcessor commandProcessor,
                                              Environment env,
                                              Function<String, String> prefix) {
        return new DisruptorCommandQueue(ringBufferSize, create(waitStrategy, env, prefix), executor, commandProcessor);
    }
    
    public static WaitStrategy create(DisruptorWaitStrategy strategy, Environment env, Function<String, String> prefix) {
        try {            
            switch(strategy) {
                case PhasedBackoffWaitStrategy: return new PhasedBackoffWaitStrategy(env.getRequiredProperty(prefix.apply("PhasedBackoffWaitStrategy.spinTimeout"), Long.class), 
                                                                                     env.getRequiredProperty(prefix.apply("PhasedBackoffWaitStrategy.yieldTimeout"), Long.class), 
                                                                                     env.getRequiredProperty(prefix.apply("PhasedBackoffWaitStrategy.unit"), TimeUnit.class), 
                                                                                     create(env.getRequiredProperty(prefix.apply("PhasedBackoffWaitStrategy.fallbackStrategy"), DisruptorWaitStrategy.class), env, prefix));
                case SleepingWaitStrategy: return new SleepingWaitStrategy(env.getProperty(prefix.apply("SleepingWaitStrategy."), Integer.class, 200));
                case TimeoutBlockingWaitStrategy: return new TimeoutBlockingWaitStrategy(env.getRequiredProperty(prefix.apply("TimeoutBlockingWaitStrategy.timeout"), Long.class), 
                                                                                         env.getRequiredProperty(prefix.apply("TimeoutBlockingWaitStrategy.unit"), TimeUnit.class));
                default: return (WaitStrategy) DisruptorCommandQueue.class.getClassLoader().loadClass(BlockingWaitStrategy.class.getPackage().getName() + "." + strategy.toString()).newInstance();
            }
        } catch(Exception e) {
            throw new RuntimeException("Could not create wait strategy: '" + strategy + "'!", e);
        }
    }
    
    public static class BucketCommandEvent {
        
        private BucketCommand<?> bucketCommand;

        public BucketCommand<?> getBucketCommand() {
            return bucketCommand;
        }
        public void setBucketCommand(BucketCommand<?> bucketCommand) {
            this.bucketCommand = bucketCommand;
        }
    }
    
    public static class BucketCommandEventFactory implements EventFactory<BucketCommandEvent> {
        @Override
        public BucketCommandEvent newInstance() {
            return new BucketCommandEvent();
        }
    }
    public static class BucketCommandEventTranslator implements EventTranslator<BucketCommandEvent> {
        
        private final BucketCommand<?> bucketCommand;
        
        public BucketCommandEventTranslator(BucketCommand<?> bucketCommand) {
            this.bucketCommand = bucketCommand;
        }
        @Override
        public void translateTo(BucketCommandEvent event, long sequence) {
            event.setBucketCommand(bucketCommand);
        }
    }
    public static class BucketCommandWorkHandler implements WorkHandler<BucketCommandEvent>, LifecycleAware {
        
        private static final Logger LOG = LoggerFactory.getLogger(DisruptorCommandQueue.BucketCommandWorkHandler.class);
        
        private final Context context;
        private final BucketCommandProcessor bucketCommandProcessor;
        
        public BucketCommandWorkHandler(BucketCommandProcessor bucketCommandProcessor) {
            this.context = new Context();
            this.bucketCommandProcessor = bucketCommandProcessor;
        }
        @Override
        public void onEvent(BucketCommandEvent event) {
            try {                
                this.bucketCommandProcessor.runCommand(event.getBucketCommand(), context);
            } catch(RuntimeException e) {
                LOG.error("Error while running command: '{}'!", event.getBucketCommand(), e);
            }
        }
        @Override
        public void onStart() {
            this.bucketCommandProcessor.init(context);
        }
        @Override
        public void onShutdown() {
            this.bucketCommandProcessor.close(context);
        }
    }
}