package de.ck35.metricstore.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;

public class Main {

    public static void main(String[] args) throws Exception {
		AbortListener.register();
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
			Resource resource = new PathResource(Paths.get(System.getProperty("user.dir"), "benchmark.properties"));
		    context.getEnvironment().getPropertySources().addFirst(new ResourcePropertySource(resource));
		    context.scan("de.ck35.metricstore.benchmark.configuration");
		    context.refresh();

		    ExecutorService executor = Executors.newFixedThreadPool(2);
		    try {
		        Monitor monitor = context.getBean(Monitor.class);
		        executor.submit(monitor);
		        monitor.awaitRun();
	            
	            Benchmark benchmark = context.getBean(Benchmark.class);
	            executor.submit(benchmark).get();
	            
	            ReadVerification readVerification = context.getBean("readVerification", ReadVerification.class);
	            executor.submit(readVerification).get();
	            
	            Reporter reporter = context.getBean(Reporter.class);
	            executor.submit(reporter).get();
		    } finally {
		        executor.shutdownNow();
		    }
		}
	}
	
	public static class AbortListener extends Thread {
		
		private static final Logger LOG = LoggerFactory.getLogger(Main.AbortListener.class);
		
		private final Thread interruptThread;
		private final InputStream stream;

		public AbortListener(Thread interruptThread, InputStream stream) {
			super("Metric-Store-Benchmark-Interrupt-Listener-Thread");
			this.interruptThread = interruptThread;
			this.stream = stream;
			this.setDaemon(true);
		}
		
		public static AbortListener register() {
		    AbortListener listener = new AbortListener(Thread.currentThread(), System.in);
		    listener.start();
			return listener;
		}
		
		@Override
		public void run() {
			while(!Thread.interrupted()) {					
				try {
					if(stream.read() == -1) {
						return;
					} else {
					    interruptThread.interrupt();
					}
				} catch (IOException e) {
					LOG.debug("AbortListener error!", e);
				}
			}
		}
	}
}