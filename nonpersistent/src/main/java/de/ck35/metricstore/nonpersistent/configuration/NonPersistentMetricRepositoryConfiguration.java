package de.ck35.metricstore.nonpersistent.configuration;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.metricstore.nonpersistent.NonPersistentMetricRepository;
import de.ck35.metricstore.util.JsonNodeExtractor;
import de.ck35.metricstore.util.TimestampFunction;

/**
 * Configuration for the {@link NonPersistentMetricRepository}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@Configuration
public class NonPersistentMetricRepositoryConfiguration {

    @Autowired Environment env;
    
    @Bean
    public NonPersistentMetricRepository nonPersistentMetricRepository() {
        return new NonPersistentMetricRepository(timestampFunction());
    }
    
    @Bean
    public Function<ObjectNode, DateTime> timestampFunction() {
        String timestampFieldName = env.getProperty("metricstore.timestamp.fieldname", TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME);
        String format = env.getProperty("metricstore.timestamp.format");
        return TimestampFunction.build(format, JsonNodeExtractor.forPath(timestampFieldName));
    }
    
}