package de.ck35.metricstore.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

public class OneShotDataGeneratorTest {

    @Test
    @Ignore
    public void testBucketInfo() {
        fail("Not yet implemented");
    }

    @Test
    @Ignore
    public void testFieldNames() {
        fail("Not yet implemented");
    }

    @Test
    public void testRandomFieldValues() {
        int valueCount = 20;
        int valueLength = 50;
        List<String> randomFieldValues = DataGenerator.randomFieldValues(valueCount, valueLength);
        assertEquals(valueCount, randomFieldValues.size());
        for(String value : randomFieldValues) {
            assertEquals("Random value was: '" + value + "'", valueLength, value.length());
        }
    }

}