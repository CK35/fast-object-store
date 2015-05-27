package de.ck35.metricstore.fs.configuration;

public class PropPrefix {

    public static String join(String propertyName) {
        return "metricrepository.fs." + propertyName;
    }
    
}