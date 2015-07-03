package de.ck35.metricstore.fs.configuration;

import com.google.common.base.Function;

/**
 * Helper which generates property keys which always start with the prefix: {@link #DEFAULT_PREFIX}.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class PropPrefix implements Function<String, String> {

    public static final String DEFAULT_PREFIX = "metricstore.fs.";
    
    private final String prefix;
    
    public PropPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public PropPrefix withPrefix(String prefix) {
        return new PropPrefix(apply(prefix));
    }
    
    @Override
    public String apply(String input) {
        return prefix + input;
    }
    
    public static String join(String propertyName) {
        return defaultPrefix().apply(propertyName);
    }

    public static PropPrefix defaultPrefix() {
        return new PropPrefix(DEFAULT_PREFIX);
    }
    
}