package de.ck35.metricstore.fs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Function;

import de.ck35.metricstore.util.MetricsIOException;

public class ObjectNodeReader implements Closeable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ObjectNodeReader.class);
	
	private final Path path;
	private final ObjectMapper mapper;
	private final BufferedReader reader;
	
	private int ignoredObjectsCount;
	
	public ObjectNodeReader(Path path, ObjectMapper mapper) throws MetricsIOException {
		this(path, mapper, Charsets.UTF_8, StandardOpenOption.READ);
	}
	public ObjectNodeReader(Path path, ObjectMapper mapper, Charset charset, OpenOption ... options) throws MetricsIOException {
		this.path = path;
		this.mapper = mapper;
		boolean closeOnError = true;
		try {
			BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(path, options));
			try {
				this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream), charset));
				closeOnError = false;
			} finally {
				if(closeOnError) {
					inputStream.close();
				}
			} 
		} catch(IOException e) {
			throw new MetricsIOException("Creating object node reader for path: '" + path + "' failed!", e);
		}
	}
	
	public ObjectNode read() throws MetricsIOException {
		try {			
			for(String line = reader.readLine() ; line != null ; line = reader.readLine()) {
				if(line.isEmpty()) {
					continue;
				}
				try {				
					return mapper.readValue(line, ObjectNode.class);
				} catch(JsonProcessingException e) {
					if(LOG.isTraceEnabled()) {					
						LOG.trace("Could not read ObjectNode from line: '{}'. Line will be ignored because of: '{}'.", line, e.getMessage());
					} else if(LOG.isDebugEnabled()){
						LOG.debug("Could not read ObjectNode from line: '{}'. Line will be ignored.", line, e);
					}
					ignoredObjectsCount++;
				}
			}
			return null;
		} catch (IOException e) {
			throw new MetricsIOException("Reading next object node from: '" + path + "' failed!", e);
		}
	}
	
	public int getIgnoredObjectsCount() {
		return ignoredObjectsCount;
	}
	public Path getPath() {
		return path;
	}
	@Override
	public void close() throws IOException {
		try {			
			this.reader.close();
		} catch(IOException e) {
			throw new IOException("Closing object node reader for: '" + path + "' failed!", e);
		}
	}
	
	public static class Factory implements Function<Path, ObjectNodeReader> {

		private final ObjectMapper mapper;
		
		public Factory(ObjectMapper mapper) {
			this.mapper = mapper;
		}
		@Override
		public ObjectNodeReader apply(Path input) {
			return new ObjectNodeReader(input, mapper);
		}
	}
}