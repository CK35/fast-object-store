package de.ck35.metricstore.util;

/**
 * Integer based setting which checks if supplied value is greater than a 
 * given minimum. In such a case the default value will also be returned. 
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public class MinimumIntSetting extends Setting<Integer> {

    private final int minValue;

    public MinimumIntSetting(int defaultValue, int minValue) {
        this(defaultValue, minValue, null);
    }
    public MinimumIntSetting(int defaultValue, int minValue, Integer optionalValue) {
        super(defaultValue, optionalValue);
        this.minValue = minValue;
    }
    @Override
    public Integer get() {
        int result = super.get();
        if(result < minValue) {
            return getDefaultValue();
        } else {
            return result;
        }
    }
}