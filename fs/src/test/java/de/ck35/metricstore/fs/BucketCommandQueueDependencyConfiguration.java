package de.ck35.metricstore.fs;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import de.ck35.metricstore.fs.BucketCommandProcessor.Context;

@Configuration
public class BucketCommandQueueDependencyConfiguration {

    @Bean
    public BucketCommandProcessor bucketCommandProcessor() {
        BucketCommandProcessor commandProcessor = mock(BucketCommandProcessor.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                BucketCommandProcessorThread.initialized();
                return null;
            }
        }).when(commandProcessor).init(any(Context.class));
        return commandProcessor;
    }
    
    @Bean
    public BucketCommandProcessorThread bucketCommandProcessorThread() {
        return new BucketCommandProcessorThread();
    }
    
}