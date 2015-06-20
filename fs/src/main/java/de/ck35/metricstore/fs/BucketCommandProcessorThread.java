package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Worker Thread for the {@link BucketCommandProcessor}. This class will be used for the {@link BucketCommandProcessor}
 * to process the {@link BucketCommandProcessor#run()} Method. The {@link #awaitInitialization()} Method
 * can be used for waiting for Processor initalization errors.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@ManagedResource
public class BucketCommandProcessorThread extends Thread implements UncaughtExceptionHandler, Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(BucketCommandProcessorThread.class);

    private static final String NAME = "ManagedBucketCommandProcessorThread";
    
    private final CountDownLatch initLatch;
    private final AtomicReference<Throwable> uncaughtExceptionRef;
    private final AtomicReference<Runnable> targetRunnableRef;
    
    public BucketCommandProcessorThread() {
        super(NAME);
        this.initLatch = new CountDownLatch(1);
        this.uncaughtExceptionRef = new AtomicReference<>();
        this.setUncaughtExceptionHandler(this);
        this.targetRunnableRef = new AtomicReference<Runnable>();
    }

    @Override
    public void run() {
        targetRunnableRef.get().run();
    }
    
    public static void initialized() {
        Thread currentThread = Thread.currentThread();
        if(currentThread instanceof BucketCommandProcessorThread) {
            ((BucketCommandProcessorThread) currentThread).initLatch.countDown();
        }
    }
    
    public void awaitInitialization() throws Throwable {
        initLatch.await();
        Throwable throwable = uncaughtExceptionRef.get();
        if(throwable != null) {
            throw throwable;
        }
    }
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Uncaught exception on Thread: '{}'!", t, e);
        uncaughtExceptionRef.set(e);
        initLatch.countDown();
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing {}.", NAME);
        uncaughtExceptionRef.set(new RuntimeException("Already closed!"));
        initLatch.countDown();
    }
    
    @ManagedAttribute
    public boolean isCommandProcessorInitialized() {
        return initLatch.getCount() == 0;
    }
    @Override
    @ManagedAttribute
    public State getState() {
        return super.getState();
    }
    
    public void setTargetRunnableRef(Runnable runnable) {
        this.targetRunnableRef.set(runnable);
    }
}