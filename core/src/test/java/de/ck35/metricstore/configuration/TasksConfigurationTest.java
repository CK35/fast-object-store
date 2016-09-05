package de.ck35.metricstore.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import de.ck35.metricstore.fs.FilesystemMetricRepository;

public class TasksConfigurationTest {

	@Test
	public void testConfigurationWithSkip() {
		System.setProperty("metricstore.tasks.skip", "true");
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestFilesystemMetricRepositoryConfiguration.class, 
		                                                                                        TasksConfiguration.class)) {
			ThreadPoolTaskScheduler taskScheduler = (ThreadPoolTaskScheduler) context.getBean("taskScheduler");
			assertEquals(0, taskScheduler.getScheduledThreadPoolExecutor().getTaskCount());
		}
	}
	
	@Test
	public void testConfiguration() {
		System.setProperty("metricstore.tasks.skip", "false");
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