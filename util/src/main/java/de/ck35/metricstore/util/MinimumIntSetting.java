package de.ck35.metricstore.util;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Integer based setting which checks if supplied value is greater than a 
 * given minimum. In such a case the default value will also be returned. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
@ManagedResource
public class MinimumIntSetting extends Setting<Integer> {

    private final int minValue;

    public MinimumIntSetting(int defaultValue, int minValue) {
        this(defaultValue, minValue, null);
    }
    public MinimumIntSetting(int defaultValue, int minValue, Integer optionalValue) {
        super(defaultValue, optionalValue);
        this.minValue = minValue;
        if(defaultValue < minValue) {
            throw new IllegalArgumentException("Default: '" + defaultValue + "' is not allowed to be smaller than min: '" + minValue + "'.");
        }
    }
    @Override
    @ManagedAttribute
    public Integer get() {
        int result = super.get();
        if(result < minValue) {
            return getDefaultValue();
        } else {
            return result;
        }
    }
    
    @ManagedAttribute
    public int getMinValue() {
        return minValue;
    }
}