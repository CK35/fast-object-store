package de.ck35.metricstore.fs.configuration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import de.ck35.metricstore.fs.FilesystemMetricRepository;
import de.ck35.metricstore.fs.configuration.PropPrefix;
import de.ck35.metricstore.fs.configuration.TasksConfiguration;

public class TasksConfigurationTest {

	@Test
	public void testConfigurationWithSkip() {
		System.setProperty(PropPrefix.join("tasks.skip"), "true");
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestFilesystemMetricRepositoryConfiguration.class, 
		                                                                                        TasksConfiguration.class)) {
			ThreadPoolTaskScheduler taskScheduler = (ThreadPoolTaskScheduler) context.getBean("taskScheduler");
			assertEquals(0, taskScheduler.getScheduledThreadPoolExecutor().getTaskCount());
		}
	}
	
	@Test
	public void testConfiguration() {
		System.setProperty(PropPrefix.join("tasks.skip"), "false");
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestFilesystemMetricRepositoryConfiguration.class, 
		                                                                                        TasksConfiguration.class)) {
			ThreadPoolTaskScheduler taskScheduler = (ThreadPoolTaskScheduler) context.getBean("taskScheduler");
			assertTrue(taskScheduler.getScheduledThreadPoolExecutor().getTaskCount() >= 2);
		}
	}

	@Configuration
	public static class TestFilesystemMetricRepositoryConfiguration {
		@Bean
		public FilesystemMetricRepository filesystemMetricRepository() {
			return mock(FilesystemMetricRepository.class);
		}
	}
}