package de.ck35.metricstore.fs;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Function;

import de.ck35.metricstore.util.MetricsIOException;

public class ObjectNodeWriter implements Closeable {

	private final Path path;
	private final JsonGenerator generator;
	private final BufferedOutputStream outputStream;
	
	public ObjectNodeWriter(Path path, JsonFactory factory) throws MetricsIOException {
		this(path, factory, Charsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}
	public ObjectNodeWriter(Path path, JsonFactory factory, Charset charset, OpenOption ... options) throws MetricsIOException {
		this.path = path;
		boolean closeOnError = true;
		try {			
			this.outputStream = new BufferedOutputStream(Files.newOutputStream(path, options));
			try {
				OutputStreamWriter writer = new OutputStreamWriter(new GZIPOutputStream(outputStream), charset);
				try {
					this.generator = factory.createGenerator(writer);
					closeOnError = false;
				} finally {
					if(closeOnError) {
						writer.close();
					}
				}
			} finally {
				if(closeOnError) {
					outputStream.close();
				}
			}
		} catch(IOException e) {
			throw new MetricsIOException("Could not create writer for: '" + path + "'!", e);
		}
	}

	public void write(ObjectNode node) throws MetricsIOException {
		try {
			generator.writeRaw('\n');
			generator.writeObject(node);			
		} catch(IOException e) {
			throw new MetricsIOException("Could not append next object node to: '" + path + "'!", e);
		}
	}
	
	public Path getPath() {
		return path;
	}
	
	@Override
	public void close() throws IOException {
		IOException exception = null;
		try {			
			this.generator.close();
		} catch(IOException e) {
			exception = e;
		}
		try {			
			this.outputStream.close();
		} catch(IOException e) {
			if(exception == null) {
				exception = e;
			}
		}
		if(exception != null) {
			throw exception;
		}
	}
	
	public static class Factory implements Function<Path, ObjectNodeWriter> {

		private final JsonFactory jsonFactory;
		
		public Factory(JsonFactory jsonFactory) {
			this.jsonFactory = jsonFactory;
		}
		@Override
		public ObjectNodeWriter apply(Path input) {
			return new ObjectNodeWriter(input, jsonFactory);
		}
	}
}