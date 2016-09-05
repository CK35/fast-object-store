package de.ck35.metricstore.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class MinimumIntSettingTest {
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructInvalidSetting() {
        new MinimumIntSetting(1, 2);
    }
    @Test
    public void testGetDefault() {
        int expected = 5;
        MinimumIntSetting setting = new MinimumIntSetting(expected, 1);
        assertEquals(expected, setting.get().intValue());
    }
    @Test
    public void testGetDefaultWhenSmaller() {
        int expected = 5;
        MinimumIntSetting setting = new MinimumIntSetting(expected, 1);
        setting.set(-1);
        assertEquals(expected, setting.get().intValue());
    }
    @Test
    public void testGetNotDefault() {
        int expected = 5;
        MinimumIntSetting setting = new MinimumIntSetting(1, 1, expected);
        assertEquals(expected, setting.get().intValue());
    }
    @Test
    public void testGetAfterSet() {
        int expected = 5;
        MinimumIntSetting setting = new MinimumIntSetting(1, 1);
        setting.set(expected);
        assertEquals(expected, setting.get().intValue());
    }
    @Test
    public void testGetAfterSetNull() {
        int expected = 5;
        MinimumIntSetting setting = new MinimumIntSetting(expected, 1, 2);
        setting.set(null);
        assertEquals(expected, setting.get().intValue());
    }

}