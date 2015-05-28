package de.ck35.metricstore.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class SettingTest {

    @Test
    public void testGetDefault() {
        String expected = "default";
        Setting<String> setting = new Setting<>(expected);
        assertEquals(expected, setting.get());
        assertEquals(expected, setting.getDefaultValue());
    }
    @Test
    public void testGetNotDefault() {
        String expected = "value";
        Setting<String> setting = new Setting<>("default", expected);
        assertEquals(expected, setting.get());
    }
    @Test
    public void testSet() {
        String expected = "value";
        Setting<String> setting = new Setting<>("default");
        setting.set(expected);
        assertEquals(expected, setting.get());
    }
    @Test
    public void testSetNull() {
        String expected = "default";
        Setting<String> setting = new Setting<>(expected, "value");
        setting.set(null);
        assertEquals(expected, setting.get());
    }

}