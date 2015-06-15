package de.ck35.metricstore.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Optional;

import de.ck35.metricstore.benchmark.Monitor.SystemState;

public class Reporter implements Callable<Void> {

	private final Monitor monitor;
    private final Path reportPath;
    private final Charset charset;
    private final String separator;
    private final DateTimeFormatter dateTimeFormatter;
    
    public Reporter(Monitor monitor,
                    Path reportPath,
                    Charset charset,
                    String separator,
                    DateTimeFormatter dateTimeFormatter) {
        this.monitor = monitor;
        this.reportPath = reportPath;
        this.charset = charset;
        this.separator = separator;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public Void call() throws InterruptedException, IOException {
        NavigableMap<DateTime, SystemState> result = monitor.awaitResult();
        try(BufferedWriter writer = Files.newBufferedWriter(reportPath, 
                                                            charset, 
                                                            StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for(Column column : Column.values()) {
                writer.write(column.toString());
                writer.write(separator);
            }
            writer.write(System.lineSeparator());
            for(Entry<DateTime, SystemState> entry : result.entrySet()) {
                for(Column column : Column.values()) {
                    column.writeValue(entry, writer, dateTimeFormatter);
                    writer.write(separator);
                }
                writer.write(System.lineSeparator());
            }
        }
    	return null;
    }
	
    enum Column {
        Timestamp() {
            @Override
            public void writeValue(Entry<DateTime, SystemState> entry,
                                   BufferedWriter writer,
                                   DateTimeFormatter dateTimeFormatter) throws IOException {
                writer.write(dateTimeFormatter.print(entry.getKey()));
            }
        },
        CPUUsage() {
            @Override
            public void writeValue(Entry<DateTime, SystemState> entry,
                                   BufferedWriter writer,
                                   DateTimeFormatter dateTimeFormatter) throws IOException {
                writeOptional(entry.getValue().getCpuUsage(), writer);
            }
        },
        HeapUsage() {
            @Override
            public void writeValue(Entry<DateTime, SystemState> entry,
                                   BufferedWriter writer,
                                   DateTimeFormatter dateTimeFormatter) throws IOException {
                writeOptional(entry.getValue().getHeapUsage(), writer);
            }
        },
        ProcessedCommandsTotal() {
            @Override
            public void writeValue(Entry<DateTime, SystemState> entry,
                                   BufferedWriter writer,
                                   DateTimeFormatter dateTimeFormatter) throws IOException {
                writeOptional(entry.getValue().getTotalProcessedCommands(), writer);
            }
        },
        ProcessedCommandsPerMinute() {
            @Override
            public void writeValue(Entry<DateTime, SystemState> entry,
                                   BufferedWriter writer,
                                   DateTimeFormatter dateTimeFormatter) throws IOException {
                writeOptional(entry.getValue().getProcessedCommandsPerSecond(), writer);
            }
        };
        
        public abstract void writeValue(Entry<DateTime, SystemState> entry, BufferedWriter writer, DateTimeFormatter dateTimeFormatter) throws IOException;
        
        public static void writeOptional(Optional<?> optional, Writer writer) throws IOException {
            if(optional.isPresent()) {
                writer.write(optional.get().toString());
            } else {
                writer.write("-");
            }
        }
    }
}