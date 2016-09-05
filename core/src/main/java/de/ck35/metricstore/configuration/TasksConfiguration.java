package de.ck35.metricstore.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import de.ck35.metricstore.fs.FilesystemMetricRepository;
import de.ck35.metricstore.fs.Tasks.CompressTask;
import de.ck35.metricstore.fs.Tasks.DeleteTask;
import de.ck35.metricstore.fs.Tasks.TasksErrorHandler;
import de.ck35.metricstore.util.MinimumIntSetting;

@Configuration
public class TasksConfiguration implements ApplicationListener<ContextRefreshedEvent> {

	public static final String DEFAULT_COMPRESS_CRON = "0 0 4 * * *";
	public static final String DEFAULT_DELETE_CRON = "0 0 6 * * *";
	public static final int DEFUALT_MAX_UNCOMPRESSED_DAYS = 2;
	public static final int DEFAULT_MAX_DAYS_TO_KEEP = 365;
	
	@Autowired Environment env;
	@Autowired FilesystemMetricRepository metricRepository;
	
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if(env.getProperty("metricstore.tasks.skip", Boolean.class, false)) {
			return;
		}
		taskScheduler().schedule(compressTask(), new CronTrigger(env.getProperty("metricstore.tasks.compress.cron", DEFAULT_COMPRESS_CRON)));
		taskScheduler().schedule(deleteTask(), new CronTrigger(env.getProperty("metricstore.tasks.delete.cron", DEFAULT_DELETE_CRON)));
	}
	
	@Bean
	public MinimumIntSetting maxUncompressedDaysSetting() {
		return new MinimumIntSetting(DEFUALT_MAX_UNCOMPRESSED_DAYS, 0, env.getProperty("metricstore.tasks.compress.max.uncompressed.days", Integer.class));
	}
	
	@Bean
	public MinimumIntSetting maxDaysToKeepSetting() {
		return new MinimumIntSetting(DEFAULT_MAX_DAYS_TO_KEEP, 0, env.getProperty("metricstore.tasks.delete.max.days.to.keep", Integer.class));
	}
	
	@Bean
	public DeleteTask deleteTask() {
		return new DeleteTask(metricRepository, maxDaysToKeepSetting());
	}
	
	@Bean
	public CompressTask compressTask() {
		return new CompressTask(metricRepository, maxUncompressedDaysSetting());
	}
	
	@Bean
	public TasksErrorHandler tasksErrorHandler() {
		return new TasksErrorHandler();
	}
	
	@Bean
	public ThreadPoolTaskScheduler taskScheduler() {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setErrorHandler(tasksErrorHandler());
		return scheduler;
	}
}