package de.ck35.metricstore.fs;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;

import de.ck35.metricstore.util.SearchableInputStream;

public class ObjectNodeReader implements Closeable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ObjectNodeReader.class);
	
	private final Path path;
	private final ReaderBasedJsonParser parser;
	private final ObjectMapper mapper;
	private final InputStreamReader reader;
	
	private Iterator<ObjectNode> iterator;
	
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
			this.reader = new InputStreamReader(new GZIPInputStream(inputStream), charset);
			try {
				this.parser = (ReaderBasedJsonParser) mapper.getFactory().createParser(reader);
				closeOnError = false;
			} finally {
				if(closeOnError) {
					reader.close();
				}
			}
		} finally {
			if(closeOnError) {
				inputStream.close();
			}
		}
	}
	
	public JsonToken next() throws IOException {
		JsonToken token = JsonToken.END_OBJECT;
		while(token != null && token != JsonToken.START_OBJECT) {			
			try {
				token = parser.nextToken();
			} catch (JsonParseException e) {
				e.printStackTrace();
				parser.clearCurrentToken();
			}
		}
		return token;
	}
	
	public ObjectNode read() throws IOException {
		ObjectNode result = null;
		while(result == null) {
			JsonToken token = next();
			if(token == null) {
				return null;
			}
			try {				
				result = parser.readValueAs(ObjectNode.class);
			} catch(JsonParseException e) {
				parser.clearCurrentToken();
				parser.skipChildren();
				ignoredObjects++;
			}
		}
		return result;
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