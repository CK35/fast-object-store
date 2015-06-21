package de.ck35.metricstore.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import de.ck35.metricstore.benchmark.Monitor.SystemState;

public class Reporter implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(Reporter.class);
    
	private final Monitor monitor;
    private final String separator;
    private final DateTimeFormatter dateTimeFormatter;
    private final Supplier<BufferedWriter> writerSupplier;
    
    public Reporter(Monitor monitor,
                    String separator,
                    DateTimeFormatter dateTimeFormatter,
                    Supplier<BufferedWriter> writerSupplier) {
        this.monitor = monitor;
        this.separator = separator;
        this.dateTimeFormatter = dateTimeFormatter;
        this.writerSupplier = writerSupplier;
    }

    @Override
    public Void call() throws InterruptedException, IOException {
        LOG.info("Creating report.");
        NavigableMap<DateTime, SystemState> result = monitor.awaitResult();
        LOG.info("Writing '{}' measurements.", result.size());
        try(BufferedWriter writer = writerSupplier.get()) {
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
            writer.write(System.lineSeparator());
            writer.write(System.lineSeparator());
        }
        LOG.info("Writing report done.");
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
        ProcessedCommandsPerSecond() {
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
                writer.write(NumberFormat.getInstance().format(optional.get()));
            } else {
                writer.write("-");
            }
        }
    }
    
    public static class FileWriterSupplier implements Supplier<BufferedWriter> {
        
        private final Path reportPath;
        private final Charset charset;
        
        public FileWriterSupplier(Path reportPath, Charset charset) {
            this.reportPath = reportPath;
            this.charset = charset;
        }
        @Override
        public BufferedWriter get() {
            try {
                return Files.newBufferedWriter(reportPath, 
                                               charset, 
                                               StandardOpenOption.CREATE, 
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new RuntimeException("Could not create writer for path: '" + reportPath + "'!");
            }
        }
    }
}