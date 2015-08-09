package de.ck35.metricstore.util.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Configuration for the shared {@link ObjectMapper}. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@Configuration
public class ObjectMapperConfiguration {

	@Bean
	public static ObjectMapper objectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
		mapper.getFactory().setRootValueSeparator(null);
		mapper.getFactory().enable(Feature.AUTO_CLOSE_TARGET);
		mapper.getFactory().disable(Feature.FLUSH_PASSED_TO_STREAM);
		return mapper;
	}
	
}