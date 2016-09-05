package de.ck35.metricstore.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

public class TimestampFunction implements Function<ObjectNode, DateTime> {

    public static final DateTimeFormatter DEFAULT_FORMATTER = ISODateTimeFormat.dateTimeParser()
																				.withOffsetParsed()
																				.withZoneUTC();
	public static final String DEFAULT_TIMESTAMP_FILED_NAME = "timestamp";
	
	private final DateTimeFormatter formatter;
	private final Function<ObjectNode, JsonNode> jsonNodeExtractor;
	
	public TimestampFunction() {
		this(DEFAULT_FORMATTER, JsonNodeExtractor.forPath(DEFAULT_TIMESTAMP_FILED_NAME));
	}
	public TimestampFunction(DateTimeFormatter formatter, Function<ObjectNode, JsonNode> jsonNodeExtractor) {
		this.formatter = formatter;
		this.jsonNodeExtractor = jsonNodeExtractor;
	}

	@Override
	public DateTime apply(ObjectNode node) {
		JsonNode jsonNode = jsonNodeExtractor.apply(node);
		if(jsonNode == null) {
			return null;
		}
		String dateTimeText = jsonNode.textValue();
		if(dateTimeText == null) {
			throw new IllegalArgumentException("Timestamp filed is missing inside object node!");
		}
		if(dateTimeText.isEmpty()) {
			throw new IllegalArgumentException("Empty String for timestamp in invalid!");
		}
		return formatter.parseDateTime(dateTimeText).withZone(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0);
	}
	
	/**
	 * Create a timestamp function with an optional datetime format and path function.
	 * 
	 * @param dateTimeFormat The format of date time fields.
	 * @param pathFunction The path function for the date time field.
	 * @return A timestamp function.
	 */
	public static TimestampFunction build(String dateTimeFormat, Function<ObjectNode, JsonNode> pathFunction) {
        final DateTimeFormatter formatter;
        if(dateTimeFormat == null) {
            formatter = TimestampFunction.DEFAULT_FORMATTER;
        } else {            
            formatter = DateTimeFormat.forPattern(dateTimeFormat).withZoneUTC();
        }
        return new TimestampFunction(formatter, pathFunction);
	}
	
}