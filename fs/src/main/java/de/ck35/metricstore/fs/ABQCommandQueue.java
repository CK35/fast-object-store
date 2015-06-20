package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

import de.ck35.metricstore.fs.BucketCommandProcessor.Context;
import de.ck35.metricstore.util.MetricsIOException;

public class ABQCommandQueue implements Runnable, Predicate<BucketCommand<?>>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ABQCommandQueue.class);
    
    private final AtomicBoolean closed;
    private final BlockingQueue<BucketCommand<?>> commands;
    private final BucketCommandProcessor commandProcessor;
    private final AtomicReference<Thread> workerThreadRef;

    public ABQCommandQueue(int commandCapacity, BucketCommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
        this.commands = new ArrayBlockingQueue<BucketCommand<?>>(commandCapacity);
        this.closed = new AtomicBoolean();
        this.workerThreadRef = new AtomicReference<>();
    }
    
    @Override
    public boolean apply(BucketCommand<?> input) {
        if(closed.get()) {
            return false;
        }
        try {
            commands.put(input);
        } catch (InterruptedException e) {
            throw new MetricsIOException("Interrupted while putting next command: '" + input + "' in queue.", e);
        }
        return true;
    }

    @Override
    public void run() {
        if(!workerThreadRef.compareAndSet(null, Thread.currentThread())) {
            throw new IllegalStateException("Started command queue worker already!");
        }
        Context context = new Context();
        try {
            commandProcessor.init(context);
            while(!closed.get() || !commands.isEmpty()) {
                try {                
                    commandProcessor.runCommand(commands.take(), context);
                } catch(InterruptedException e) {
                    LOG.debug("Interrupted while waiting for next command in queue.");
                }
            }
        } finally {
            Thread.interrupted();
            commandProcessor.close(context);
        }
    }
    
    @Override
    public void close() {
        this.closed.set(true);
        Thread thread = workerThreadRef.get();
        if(thread != null) {
            thread.interrupt();
        }
    }
    
}