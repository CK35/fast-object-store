package de.ck35.metricstore.fs.configuration;

/**
 * Helper which generates property keys which always start with the prefix: {@link #PREFIX}.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class PropPrefix {

    public static final String PREFIX = "metricrepository.fs.";
    
    public static String join(String propertyName) {
        return PREFIX + propertyName;
    }
    
}