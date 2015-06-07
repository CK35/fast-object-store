package de.ck35.metricstore.fs;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import de.ck35.metricstore.fs.BucketCommandProcessor.Context;
import de.ck35.metricstore.fs.configuration.BucketCommandQueueConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={BucketCommandQueueDependencyConfiguration.class,
                               BucketCommandQueueConfiguration.class})
public class DisruptorCommandQueueTest {

    @BeforeClass
    public static void beforeContext() {
        System.setProperty("metricrepository.fs.commands.queue.mode", "DISRUPTOR");
    }
    
    @Autowired ConfigurableApplicationContext context;
    @Autowired DisruptorCommandQueue disruptorCommandQueue;
    @Autowired BucketCommandProcessor bucketCommandProcessor;
    
    @Test
    public void testDisruptor() {
        BucketCommand<?> bucketCommand = mock(BucketCommand.class);
        for(int i=0 ; i<10 ; i++) {            
            assertTrue(disruptorCommandQueue.apply(bucketCommand));
        }
        verify(bucketCommandProcessor, timeout(2000).times(10)).runCommand(eq(bucketCommand), any(Context.class));
        reset(bucketCommandProcessor);
        
        doThrow(IllegalArgumentException.class).when(bucketCommandProcessor).runCommand(eq(bucketCommand), any(Context.class));
        assertTrue(disruptorCommandQueue.apply(bucketCommand));
        verify(bucketCommandProcessor, timeout(2000)).runCommand(eq(bucketCommand), any(Context.class));
        
        context.close();
        verify(bucketCommandProcessor, timeout(200)).close(any(Context.class));
    }

}