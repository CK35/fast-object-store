package de.ck35.metricstore.util;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Splitter;

public class JsonNodeExtractor {

	public static Function<ObjectNode, JsonNode> forPath(String nodePath) {
		if(nodePath == null || nodePath.trim().isEmpty()) {
			throw new IllegalArgumentException("Empty node path is not allowed!");
		}
		Splitter splitter = Splitter.on(".").trimResults().omitEmptyStrings();
		List<String> tokens = splitter.splitToList(nodePath);
		if(tokens.isEmpty()) {
			throw new IllegalArgumentException("No tokens found inside node path: '" + nodePath + "'.");
		} else if(tokens.size() == 1) {
			return new OneTokenExtractor(tokens.get(0));
		} else if(tokens.size() == 2) {
			return new TwoTokensExtractor(tokens.get(0), tokens.get(1));
		} else if(tokens.size() == 3) {
			return new ThreeTokensExtractor(tokens.get(0), tokens.get(1), tokens.get(2));
		} else {
			return new NTokensExtractor(tokens);
		}
	}
	
	public static class OneTokenExtractor implements Function<ObjectNode, JsonNode> {
		
		private final String fieldName;
		
		public OneTokenExtractor(String fieldName) {
			this.fieldName = Objects.requireNonNull(fieldName);
		}
		@Override
		public JsonNode apply(ObjectNode input) {
			return input == null ? null : input.path(fieldName);
		}
	}
	
	public static class TwoTokensExtractor implements Function<ObjectNode, JsonNode> {
		
		private final String firstFieldName;
		private final String secondFieldName;
		
		public TwoTokensExtractor(String firstFieldName, String secondFieldName) {
			this.firstFieldName = firstFieldName;
			this.secondFieldName = secondFieldName;
		}
		@Override
		public JsonNode apply(ObjectNode input) {
			return input == null ? null : input.path(firstFieldName).path(secondFieldName);
		}
	}
	
	public static class ThreeTokensExtractor implements Function<ObjectNode, JsonNode> {
		
		private final String firstFieldName;
		private final String secondFieldName;
		private final String thirdFieldName;
		
		public ThreeTokensExtractor(String firstFieldName, String secondFieldName, String thirdFieldName) {
			this.firstFieldName = firstFieldName;
			this.secondFieldName = secondFieldName;
			this.thirdFieldName = thirdFieldName;
		}
		@Override
		public JsonNode apply(ObjectNode input) {
			return input == null ? null : input.path(firstFieldName).path(secondFieldName).path(thirdFieldName);
		}
	}
	
	public static class NTokensExtractor implements Function<ObjectNode, JsonNode> {
		
		private final Iterable<String> fieldNames;
		
		public NTokensExtractor(Iterable<String> fieldNames) {
			this.fieldNames = fieldNames;
		}
		@Override
		public JsonNode apply(ObjectNode input) {
			if(input == null) {
				return null;
			}
			JsonNode currentNode = input;
			for(String fieldName : fieldNames) {
				currentNode = currentNode.path(fieldName);
			}
			return currentNode;
		}
	}
}