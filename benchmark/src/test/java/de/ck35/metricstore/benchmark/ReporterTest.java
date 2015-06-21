package de.ck35.metricstore.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;

import de.ck35.metricstore.benchmark.Monitor.SystemState;

public class ReporterTest {

    @Test
    public void testCall() throws IOException, InterruptedException {
        NavigableMap<DateTime, SystemState> result = new TreeMap<>();
        result.put(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), new SystemState(Optional.of(1d), 
                                                                                     Optional.of(2d), 
                                                                                     Optional.of(3l), 
                                                                                     Optional.<Double>absent()));
        Monitor monitor = mock(Monitor.class);
        try(StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter)) {
            Reporter reporter = new Reporter(monitor, ";", DateTimeFormat.forPattern("HH:mm"), Suppliers.ofInstance(writer));
            when(monitor.awaitResult()).thenReturn(result);
            reporter.call();
            
            List<String> lines = Splitter.on(System.lineSeparator()).splitToList(stringWriter.getBuffer());
            assertEquals(5, lines.size());
            assertEquals("Timestamp;CPUUsage;HeapUsage;ProcessedCommandsTotal;ProcessedCommandsPerSecond;", lines.get(0));
            assertEquals("00:00;1;2;3;-;", lines.get(1));
            assertTrue(lines.get(2).isEmpty());
            assertTrue(lines.get(3).isEmpty());
        }
    }

}