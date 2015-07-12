package de.ck35.metricstore.fs.configuration;

import java.nio.file.Path;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Function;

import de.ck35.metricstore.fs.WritableFilesystemBucketFactory;
import de.ck35.metricstore.util.JsonNodeExtractor;
import de.ck35.metricstore.util.MinimumIntSetting;
import de.ck35.metricstore.util.TimestampFunction;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

/**
 * Configuration for the {@link WritableFilesystemBucketFactory}.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@Configuration
public class WritableFilesystemBucketFactoryConfiguration {

    public static int DEFAULT_CACHED_WRITERS_COUNT = 5;
    
    @Autowired Environment env;
    @Autowired ObjectMapper mapper;
    
    @Bean
    public MinimumIntSetting cachedWritersCount() {
        return new MinimumIntSetting(DEFAULT_CACHED_WRITERS_COUNT, 1, env.getProperty(PropPrefix.join("cached.writers.count"), Integer.class, null));
    }
    
    @Bean
    public Function<ObjectNode, DateTime> timestampFunction() {
        String timestampFieldName = env.getProperty("metricstore.timestamp.fieldname", TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME);
        String format = env.getProperty("metricstore.timestamp.format");
        return TimestampFunction.build(format, JsonNodeExtractor.forPath(timestampFieldName));
    }
    
    @Bean
    public WritableFilesystemBucketFactory writableFilesystemBucketFactory() {
        return new WritableFilesystemBucketFactory(timestampFunction(), 
                                                   writerFactory(), 
                                                   readerFactory(), 
                                                   cachedWritersCount());
    }

    @Bean
    public Function<Path, ObjectNodeReader> readerFactory() {
        return new ObjectNodeReader.PathFactory(mapper, Charsets.UTF_8);
    }

    @Bean
    public Function<Path, ObjectNodeWriter> writerFactory() {
        return new ObjectNodeWriter.PathFactory(mapper.getFactory(), Charsets.UTF_8);
    }
    
}