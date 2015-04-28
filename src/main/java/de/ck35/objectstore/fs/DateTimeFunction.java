package de.ck35.objectstore.fs;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

public class DateTimeFunction implements Function<ObjectNode, DateTime> {

	private static final DateTimeFormatter DEFAULT_FORMATTER = ISODateTimeFormat.dateTimeParser()
																				.withOffsetParsed()
																				.withZoneUTC();
	protected static final String DEFAULT_TIMESTAMP_FILED_NAME = "timestamp";
	
	private final DateTimeFormatter formatter;
	private final String timestampFieldName;
	
	public DateTimeFunction() {
		this(DEFAULT_FORMATTER, DEFAULT_TIMESTAMP_FILED_NAME);
	}
	public DateTimeFunction(DateTimeFormatter formatter, String timestampFieldName) {
		this.formatter = formatter;
		this.timestampFieldName = timestampFieldName;
	}

	@Override
	public DateTime apply(ObjectNode node) {
		String dateTimeText = node.path(timestampFieldName).textValue();
		if(dateTimeText == null) {
			throw new IllegalArgumentException("Timestamp filed: '" + timestampFieldName + "' is missing!");
		}
		if(dateTimeText.isEmpty()) {
			throw new IllegalArgumentException("Empty String for timestamp in invalid!");
		}
		return formatter.parseDateTime(dateTimeText).withZone(DateTimeZone.UTC);
	}
	
}