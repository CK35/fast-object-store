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
import java.util.Objects;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.ck35.metricstore.util.io.MetricsIOException;

/**
 * Allows iteration of path child elements (directories and files) in a numeric sorted manner.
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class NumericSortedPathIterable implements Iterable<Entry<Integer, Path>>, 
												  Function<Path, Entry<Integer, Path>>, 
												  Filter<Path>, 
												  Comparator<Entry<Integer, Path>> {
	
	private static final Function<Path, Integer> DEFAULT_NUMBER_FUNCTION = new DefaultNumberFunction();
	
	private final Path parent;
	private final Function<Path, Integer> numberFunction;
	
	/**
	 * Construct the iterable for the given parent path.
	 * 
	 * @param parent The parent path normally points to a existing directory.
	 */
	public NumericSortedPathIterable(Path parent) {
		this(parent, DEFAULT_NUMBER_FUNCTION);
	}
	
	/**
	 * Construct the iterable for the given parent path and number extract function.
	 * 
	 * @param parent The parent path normally points to a existing directory.
	 * @param numberFunction Function for extracting a integer from a provided path.
	 */
	public NumericSortedPathIterable(Path parent, Function<Path, Integer> numberFunction) {
		this.parent = Objects.requireNonNull(parent);
		this.numberFunction = Objects.requireNonNull(numberFunction);
	}
	
	@Override
	public Entry<Integer, Path> apply(Path input) {
		return input == null ? null : Maps.immutableEntry(numberFunction.apply(input), input);
	}
	
	@Override
	public Iterator<Entry<Integer, Path>> iterator() {
		try(DirectoryStream<Path> stream = Files.newDirectoryStream(parent, this)) {
			List<Entry<Integer, Path>> content = Lists.newArrayList(Iterables.transform(stream, this));
			Collections.sort(content, this);
			return content.iterator();
		} catch(IOException e) {
			throw new MetricsIOException("Could not list content of: '" + parent + "'!", e);
		}
	}
	
	@Override
	public boolean accept(Path entry) throws IOException {
		try {						
			numberFunction.apply(entry);
			return true;
		} catch(IllegalArgumentException e) {
			return false;
		}
	}
	@Override
	public int compare(Entry<Integer, Path> entry1, Entry<Integer, Path> entry2) {
		return Integer.compare(entry1.getKey(), entry2.getKey());
	}
	
	public static class DefaultNumberFunction implements Function<Path, Integer> {
		
		@Override
		public Integer apply(Path input) {
			return input == null ? null : intFromFileName(input);
		}
		
		/**
		 * Convert the file name of the given path into an integer by converting the file name.
		 * 
		 * @param input The path from which the integer based name should be read and converted. 
		 * @return The converted integer value taken from the provided file name.
		 * @throws NumberFormatException If file name could not be converted into an integer.
		 */
		public static int intFromFileName(Path input) throws NumberFormatException {
			return Integer.parseInt(input.getFileName().toString());
		}
	}
}