package de.ck35.metricstore.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketData {

	private static final Logger LOG = LoggerFactory.getLogger(BucketData.class);
	
	private static final String TYPE_SUFFIX = ".type";
	
	private final Path basePath;
	private final String name;
	private final String type;
	
	public BucketData(Path basePath, String name, String type) {
		this.basePath = basePath;
		this.name = name;
		this.type = type;
	}
	
	public Path getBasePath() {
		return basePath;
	}
	public String getName() {
		return name;
	}
	public String getType() {
		return type;
	}
	
	@Override
	public String toString() {
		return "BucketData [basePath=" + basePath + ", name=" + name + ", type=" + type + "]";
	}

	public static BucketData load(Path basePath) throws IOException {
		if(!Files.isDirectory(basePath)) {
			throw new IOException("Base path is not a directory.");
		}
		String name = basePath.getFileName().toString();
		String type;
		try(DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath, "*" + TYPE_SUFFIX)) {
			Iterator<Path> iterator = directoryStream.iterator();
			if(iterator.hasNext()) {
				String fileName = iterator.next().getFileName().toString();
				type = fileName.substring(0, fileName.length()-TYPE_SUFFIX.length());
			} else {
				type = null;
			}
		}
		return new BucketData(basePath, name, type);
	}
	
	public static BucketData create(Path parent, String name, String type) throws IOException {
		Path basePath = Files.createDirectories(parent.resolve(Objects.requireNonNull(name)));
		if(type != null) {
			Path typeFilePath = basePath.resolve(type + TYPE_SUFFIX);
			try {				
				Files.createFile(typeFilePath);
			} catch(FileAlreadyExistsException e) {
				LOG.debug("Type file already exists for bucket path: '{}'.", typeFilePath);
			}
		}
		return new BucketData(basePath, name, type);
	}
}