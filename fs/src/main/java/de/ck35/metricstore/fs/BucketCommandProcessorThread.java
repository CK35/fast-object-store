package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketCommandProcessorThread extends Thread implements UncaughtExceptionHandler, Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(BucketCommandProcessorThread.class);

    private static final String NAME = "ManagedBucketCommandProcessorThread";
    
    private final CountDownLatch initLatch;
    private final AtomicReference<Throwable> uncaughtExceptionRef;
    
    public BucketCommandProcessorThread(Runnable runnable) {
        super(runnable, NAME);
        this.initLatch = new CountDownLatch(1);
        this.uncaughtExceptionRef = new AtomicReference<>();
        this.setUncaughtExceptionHandler(this);
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
        uncaughtExceptionRef.set(new InterruptedException());
        initLatch.countDown();
        interrupt();
    }
}