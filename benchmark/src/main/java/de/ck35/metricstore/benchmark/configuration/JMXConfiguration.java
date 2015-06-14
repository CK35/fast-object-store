package de.ck35.metricstore.benchmark.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.jmx.export.annotation.AnnotationJmxAttributeSource;
import org.springframework.jmx.export.assembler.MBeanInfoAssembler;
import org.springframework.jmx.export.assembler.MetadataMBeanInfoAssembler;
import org.springframework.jmx.export.metadata.JmxAttributeSource;
import org.springframework.jmx.export.naming.MetadataNamingStrategy;
import org.springframework.jmx.export.naming.ObjectNamingStrategy;
import org.springframework.jmx.support.MBeanServerFactoryBean;

@Configuration
public class JMXConfiguration {

	@Bean
	public MBeanServerFactoryBean mbeanServerFactoryBean() {
		MBeanServerFactoryBean serverFactoryBean = new MBeanServerFactoryBean();
		serverFactoryBean.setLocateExistingServerIfPossible(true);
		return serverFactoryBean;
	}
	
	@Bean
	public MBeanExporter exporter() {
		MBeanExporter exporter = new MBeanExporter();
		exporter.setAssembler(assembler());
		exporter.setNamingStrategy(namingStrategy());
		return exporter;
	}
	
	@Bean
	public MBeanInfoAssembler assembler() {
		return new MetadataMBeanInfoAssembler(attributeSource());
	}
	
	@Bean
	public JmxAttributeSource attributeSource() {
		return new AnnotationJmxAttributeSource();
	}
	
	@Bean
	public ObjectNamingStrategy namingStrategy() {
		return new MetadataNamingStrategy(attributeSource());
	}
	
}