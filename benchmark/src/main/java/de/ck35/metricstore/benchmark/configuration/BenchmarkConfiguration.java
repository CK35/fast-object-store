package de.ck35.metricstore.benchmark.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.benchmark.Benchmark;

@Configuration
@ComponentScan(basePackages={"de.ck35.metricstore.fs.configuration"})
public class BenchmarkConfiguration {

	@Autowired MetricRepository repository;
	@Autowired ObjectMapper mapper;
	
	@Bean
	public Benchmark benchmark() {
		return new Benchmark(repository, mapper);
	}
	
}