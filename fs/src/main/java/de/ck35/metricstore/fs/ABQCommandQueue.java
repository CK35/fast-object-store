package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicate;

import de.ck35.metricstore.fs.BucketCommandProcessor.Context;
import de.ck35.metricstore.util.MetricsIOException;

public class ABQCommandQueue implements Runnable, Predicate<BucketCommand<?>>, Closeable {

    private final AtomicBoolean closed;
    private final BlockingQueue<BucketCommand<?>> commands;
    private final BucketCommandProcessor commandProcessor;

    public ABQCommandQueue(int commandCapacity, BucketCommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
        this.commands = new ArrayBlockingQueue<BucketCommand<?>>(commandCapacity);
        this.closed = new AtomicBoolean();
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
        Context context = new Context();
        try {
            commandProcessor.init(context);
            BucketCommandProcessorThread.initialized();
            try {                
                while(!Thread.interrupted()) {
                    commandProcessor.runCommand(commands.take(), context);
                }
            } catch(InterruptedException e) {
                
            }
        } finally {
            commandProcessor.close(context);
        }
    }
    
    @Override
    public void close() {
        this.closed.set(true);
    }
    
}