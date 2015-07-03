package de.ck35.metricstore.util.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

public class GzipUtils {

	public static void gzip(Path source, Path destination) throws IOException {
		try(InputStream in = new BufferedInputStream(Files.newInputStream(source));
			OutputStream out = new BufferedOutputStream(Files.newOutputStream(destination))) {
			gzip(in, out);
		}
	}
	
	public static void gzip(InputStream source, OutputStream destination) throws IOException {
		try(GZIPOutputStream gzOut = new GZIPOutputStream(destination)) {
			for(int next = source.read() ; next != -1 ; next = source.read()) {
				gzOut.write(next);
			}
		}
	}
}