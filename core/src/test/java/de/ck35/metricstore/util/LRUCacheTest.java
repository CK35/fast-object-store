package de.ck35.metricstore.util;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import de.ck35.metricstore.util.LRUCache;

public class LRUCacheTest {

	@Test
	public void testReplace() {
		LRUCache<String, String> cache = new LRUCache<>(1);
		cache.put("a", "a1");
		assertEquals(ImmutableList.of("a1"), ImmutableList.copyOf(cache.put("a", "a2")));
	}
	
	@Test
	@SuppressWarnings("unchecked")
    public void testReplaceAfterResize() {
        Supplier<Integer> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(5);
        LRUCache<String, String> cache = new LRUCache<>(supplier);
        cache.put("1", "a1");
        cache.put("2", "a2");
        cache.put("3", "a3");
        cache.put("4", "a4");
        cache.put("5", "a5");
        when(supplier.get()).thenReturn(2);
        assertEquals(ImmutableList.of("a1","a2","a3","a4"), ImmutableList.copyOf(cache.put("6", "a6")));
        assertEquals(ImmutableSet.of("a5", "a6"), ImmutableSet.copyOf(cache));
    }
	
	@Test(expected=NullPointerException.class)
	public void testPutNull() {
		LRUCache<String, String> cache = new LRUCache<>(1);
		cache.put("a", null);
	}
	
	@Test
	public void testPut() {
		LRUCache<String, String> cache = new LRUCache<>(Suppliers.ofInstance(3));
		cache.put("a", "a1");
		cache.put("b", "b1");
		cache.put("c", "c1");
		assertEquals(3, cache.size());
		assertEquals(ImmutableSet.of("a1", "b1", "c1"), ImmutableSet.copyOf(cache));
	}
	
	@Test
	public void testPutRemoveFirst() {
		LRUCache<String, String> cache = new LRUCache<>(Suppliers.ofInstance(3));
		cache.put("a", "a1");
		cache.put("b", "b1");
		cache.put("c", "c1");
		cache.put("d", "d1");
		assertEquals(3, cache.size());
		assertEquals(ImmutableSet.of("b1", "c1", "d1"), ImmutableSet.copyOf(cache));
	}
	
	@Test
	public void testPutRemoveWithUpdate() {
		LRUCache<String, String> cache = new LRUCache<>(Suppliers.ofInstance(3));
		cache.put("a", "a1");
		cache.put("b", "b1");
		cache.put("c", "c1");
		cache.get("a");
		cache.put("d", "d1");
		assertEquals(3, cache.size());
		assertEquals(ImmutableSet.of("a1", "c1", "d1"), ImmutableSet.copyOf(cache));
	}

	@Test
	public void testRemove() {
		LRUCache<String, String> cache = new LRUCache<>(1);
		cache.put("a", "a1");
		assertEquals("a1", cache.remove("a"));
	}
	
	@Test
	public void testRemoveNotExisiting() {
		LRUCache<String, String> cache = new LRUCache<>(1);
		cache.put("a", "a1");
		assertNull(cache.remove("b"));
	}

	@Test
	public void testClear() {
		LRUCache<String, String> cache = new LRUCache<>(1);
		cache.put("a", "a1");
		assertEquals("a1", cache.get("a"));
		cache.clear();
		assertNull(cache.get("a"));
	}
	
}