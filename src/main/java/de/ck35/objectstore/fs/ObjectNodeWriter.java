package de.ck35.objectstore.fs;

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

public class ObjectNodeWriter implements Closeable {

	private final Path path;
	private final JsonGenerator generator;
	private final BufferedOutputStream outputStream;
	
	public ObjectNodeWriter(Path path, JsonFactory factory) throws IOException {
		this(path, factory, Charsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}
	public ObjectNodeWriter(Path path, JsonFactory factory, Charset charset, OpenOption ... options) throws IOException {
		this.path = path;
		boolean closeOnError = true;
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
	}

	public void write(ObjectNode node) throws IOException {
		generator.writeRaw('\n');
		generator.writeObject(node);
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
	
}