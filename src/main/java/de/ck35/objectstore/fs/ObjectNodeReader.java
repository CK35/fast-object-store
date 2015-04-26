package de.ck35.objectstore.fs;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;

public class ObjectNodeReader implements Closeable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ObjectNodeReader.class);
	
	private final Path path;
	private final ObjectMapper mapper;
	private final BufferedReader reader;
	
	private int ignoredObjects;
	
	public ObjectNodeReader(Path path, ObjectMapper mapper) throws IOException {
		this(path, mapper, Charsets.UTF_8, StandardOpenOption.READ);
	}
	public ObjectNodeReader(Path path, ObjectMapper mapper, Charset charset, OpenOption ... options) throws IOException {
		this.path = path;
		this.mapper = mapper;
		boolean closeOnError = true;
		BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(path, options));
		try {
			this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream), charset));
			closeOnError = false;
		} finally {
			if(closeOnError) {
				inputStream.close();
			}
		}
	}
	
	public ObjectNode read() throws IOException {
		for(String line = reader.readLine() ; line !=null ; line = reader.readLine()) {
			if(line.isEmpty()) {
				continue;
			}
			try {				
				JsonNode node = mapper.readTree(line);
				if(node instanceof ObjectNode) {
					return (ObjectNode) node;
				} else {
					throw new IOException("Not an Object node!");
				}
			} catch(IOException e) {
				ignoredObjects++;
				if(LOG.isDebugEnabled()) {
					LOG.warn("Ignoring not readable json in file: '{}': '{}'.", path, line, e);
				} else {					
					LOG.warn("Ignoring not readable json in file: '{}': '{}'. Caused by: {}", path, line, e.getMessage());
				}
			}
		}
		return null;
	}
	
	public int getIgnoredObjects() {
		return ignoredObjects;
	}
	
	public Path getPath() {
		return path;
	}
	
	@Override
	public void close() throws IOException {
		this.reader.close();
	}

}