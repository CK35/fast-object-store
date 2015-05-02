package de.ck35.metricstore.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class SearchableInputStream extends InputStreamReader {

	private Integer next;
	
	public SearchableInputStream(InputStream source, Charset charset) {
		super(source, charset);
	}
	
	public boolean search(int value) throws IOException {
		if(next == null) {
			next = super.read();
		}
		while(next != -1) {
			if(next == value) {
				return true;
			}
			read();
		}
		return false;
	}
	
	@Override
	public int read() throws IOException {
		if(next == null) {
			next = super.read();
		}
		int result = next;
		next = super.read();
		return result;
	}
	

}