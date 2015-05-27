package de.ck35.metricstore.fs;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;
import de.ck35.metricstore.fs.BucketCommand.ListBucketsCommand;
import de.ck35.metricstore.fs.BucketCommand.ReadCommand;
import de.ck35.metricstore.fs.BucketCommand.WriteCommand;
import de.ck35.metricstore.util.MinimumIntSetting;

@RunWith(MockitoJUnitRunner.class)
public class FilesystemMetricRepositoryTest {

    private MinimumIntSetting readBufferSizeSetting;
    @Mock BlockingQueue<BucketCommand<?>> commands;
    
    @Before
    public void before() {
        this.readBufferSizeSetting = new MinimumIntSetting(10_000, 1);
    }
    
    public FilesystemMetricRepository filesystemMetricRepository() {
        return new FilesystemMetricRepository(commands, readBufferSizeSetting);
    }
    
    @Test
    public void testAppendCommand() throws InterruptedException {
        FilesystemMetricRepository repository = filesystemMetricRepository();
        BucketCommand<?> command = mock(BucketCommand.class);
        assertEquals(command, repository.appendCommand(command));
        verify(commands).put(command);
    }
    
    @Test(expected=RuntimeException.class)
    public void testAppendCommandFails() throws InterruptedException {
        BucketCommand<?> command = mock(BucketCommand.class);
        doThrow(InterruptedException.class).when(commands).put(command);
        FilesystemMetricRepository repository = filesystemMetricRepository();
        repository.appendCommand(command);
    }

    @Test
    public void testListBuckets() throws InterruptedException {
        assertFalse(Thread.interrupted());
        List<MetricBucket> expected = Arrays.asList(mock(MetricBucket.class));
        BucketCommandAnswer commandAnswer = new BucketCommandAnswer(expected);
        doAnswer(commandAnswer).when(commands).put(any(BucketCommand.class));
        assertEquals(expected, filesystemMetricRepository().listBuckets());
        assertTrue(commandAnswer.getCommand().isPresent());
        BucketCommand<?> bucketCommand = commandAnswer.getCommand().get();
        assertTrue(bucketCommand instanceof ListBucketsCommand);
    }

    @Test
    public void testWirte() throws InterruptedException {
        StoredMetric expected = mock(StoredMetric.class);
        BucketCommandAnswer commandAnswer = new BucketCommandAnswer(expected);
        doAnswer(commandAnswer).when(commands).put(any(BucketCommand.class));
        String bucketName = "a";
        String bucketType = "b";
        ObjectNode objectNode = new ObjectMapper().getNodeFactory().objectNode();
        assertEquals(expected, filesystemMetricRepository().wirte(bucketName, bucketType, objectNode));
        assertTrue(commandAnswer.getCommand().isPresent());
        BucketCommand<?> bucketCommand = commandAnswer.getCommand().get();
        assertTrue(bucketCommand instanceof WriteCommand);
        WriteCommand writeCommand = (WriteCommand) bucketCommand;
        assertEquals(bucketName, writeCommand.getBucketName());
        assertEquals(bucketType, writeCommand.getBucketType());
        assertEquals(objectNode, writeCommand.getNode());
    }

    @Test
    public void testRead() throws InterruptedException {
        FilesystemMetricRepository repository = filesystemMetricRepository();
        List<StoredMetric> metrics = ImmutableList.of(mock(StoredMetric.class));
        ReadCommandAnswer commandAnswer = new ReadCommandAnswer(metrics);
        doAnswer(commandAnswer).when(commands).put(any(BucketCommand.class));
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        String bucketName = "a";
        Interval interval = new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), Period.minutes(1));
        repository.read(bucketName, interval, callable);
        ArgumentCaptor<StoredMetric> captor = ArgumentCaptor.forClass(StoredMetric.class);
        verify(callable).call(captor.capture());
        assertEquals(metrics, captor.getAllValues());
        
        assertTrue(commandAnswer.getCommand().isPresent());
        BucketCommand<?> bucketCommand = commandAnswer.getCommand().get();
        assertTrue(bucketCommand instanceof ReadCommand);
        ReadCommand readCommand = (ReadCommand) bucketCommand;
        assertEquals(bucketName, readCommand.getBucketName());
        assertEquals(interval, readCommand.getInterval());
    }

    public static class BucketCommandAnswer implements Answer<Void> {
        
        private final Object result;
        
        private Optional<BucketCommand<?>> command;
        
        public BucketCommandAnswer(Object result) {
            this.result = result;
            this.command = Optional.absent();
        }
        @Override
        public Void answer(InvocationOnMock invocation) {
            BucketCommand<?> bucketCommand = invocation.getArgumentAt(0, BucketCommand.class);
            bucketCommand.setResult(result);
            bucketCommand.commandCompleted();
            setCommand(bucketCommand);
            return null;
        }
        public void setCommand(BucketCommand<?> command) {
            this.command = Optional.<BucketCommand<?>>of(command);
        }
        public Optional<BucketCommand<?>> getCommand() {
            return command;
        }
    }
    
    public static class ReadCommandAnswer extends BucketCommandAnswer {
        
        private final Iterable<StoredMetric> storedMetrics;
        
        public ReadCommandAnswer(Iterable<StoredMetric> storedMetrics) {
            super(null);
            this.storedMetrics = storedMetrics;
        }
        @Override
        public Void answer(InvocationOnMock invocation) {
            ReadCommand readCommand = invocation.getArgumentAt(0, ReadCommand.class);
            Predicate<StoredMetric> predicate = readCommand.getPredicate();
            for(StoredMetric metric : storedMetrics) {
                assertTrue(predicate.apply(metric));
            }
            readCommand.commandCompleted();
            setCommand(readCommand);
            return null;
        }
    }
}