package de.ck35.metricstore.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream.Filter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import de.ck35.metricstore.util.MetricsIOException;

/**
 * Allows iteration of path child elements (directories or files) in a numeric sorted manner.
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class NumericSortedPathIterable implements Iterable<Path>, Function<Path, Integer>, Filter<Path>, Comparator<Path> {
	
	private final Path parent;
	private final boolean listDirectories;
	
	/**
	 * Construct the iterable for the given parent path.
	 * 
	 * @param parent The parent path normally points to a existing directory.
	 * @param listDirectories <code>true</code> if directories should be iterated.
	 */
	public NumericSortedPathIterable(Path parent, boolean listDirectories) {
		this.parent = parent;
		this.listDirectories = listDirectories;
	}
	
	/**
	 * Convert the file name of the given path into an integer.
	 * 
	 * @param input The path from which the integer based name should be read and converted. 
	 * @return The converted integer value taken from the provided file name.
	 * @throws NumberFormatException If file name could not be converted into an integer.
	 */
	public static int intFromFileName(Path input) throws NumberFormatException {
		return Integer.parseInt(input.getFileName().toString());
	}
	
	@Override
	public Iterator<Path> iterator() {
		try(DirectoryStream<Path> stream = Files.newDirectoryStream(parent, this)) {
			List<Path> content = Lists.newArrayList(stream);
			Collections.sort(content, this);
			return content.iterator();
		} catch(IOException e) {
			throw new MetricsIOException("Could not list content of: '" + parent + "'!", e);
		}
	}
	@Override
	public Integer apply(Path input) {
		return input == null ? null : intFromFileName(input);
	}
	@Override
	public boolean accept(Path entry) throws IOException {
		if((listDirectories && Files.isDirectory(entry)) || (!listDirectories && Files.isRegularFile(entry))) {
			try {						
				apply(entry);
				return true;
			} catch(NumberFormatException e) {
				return false;
			}
		}
		return false;
	}
	@Override
	public int compare(Path o1, Path o2) {
		return Integer.compare(apply(o1), apply(o2));
	}
}