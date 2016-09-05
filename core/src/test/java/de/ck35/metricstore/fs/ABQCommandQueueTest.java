package de.ck35.metricstore.fs;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import de.ck35.metricstore.fs.BucketCommandProcessor.Context;

public class ABQCommandQueueTest {

    @Test
    public void testApplyCommandsAfterShutdown() {
        BucketCommand<?> command1 = mock(BucketCommand.class);
        BucketCommand<?> command2 = mock(BucketCommand.class);
        BucketCommandProcessor commandProcessor = mock(BucketCommandProcessor.class);
        ABQCommandQueue queue = new ABQCommandQueue(10, commandProcessor);
        queue.apply(command1);
        queue.apply(command2);
        queue.close();
        queue.run();
        verify(commandProcessor, times(2)).runCommand(any(BucketCommand.class), any(Context.class));
    }

    @Test
    public void testRun() {
        BucketCommand<?> command = mock(BucketCommand.class);
        BucketCommandProcessor commandProcessor = mock(BucketCommandProcessor.class);
        final ABQCommandQueue queue = new ABQCommandQueue(10, commandProcessor);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                queue.close();
                return null;
            }
        }).when(commandProcessor).runCommand(eq(command), any(Context.class));
        queue.apply(command);
        queue.run();
        verify(commandProcessor, times(1)).runCommand(any(BucketCommand.class), any(Context.class));
    }

}