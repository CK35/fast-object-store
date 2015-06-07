package de.ck35.metricstore.benchmark;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import de.ck35.metricstore.benchmark.configuration.BenchmarkConfiguration;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		AbortListener.register();
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BenchmarkConfiguration.class)) {
			Monitor monitor = context.getBean("monitor", Monitor.class);
			monitor.start();
			
			Thread benchmarkThread = context.getBean("benchmark", Thread.class);
			benchmarkThread.start();
			benchmarkThread.join();
			
			Thread reporter = context.getBean("reporter", Thread.class);
			reporter.start();
			reporter.join();
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
		
		public static void register() {
			new AbortListener(Thread.currentThread(), System.in).start();
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