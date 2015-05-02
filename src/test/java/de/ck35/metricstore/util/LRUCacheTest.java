package de.ck35.metricstore.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import de.ck35.metricstore.util.LRUCache;

public class LRUCacheTest {

	@Test
	public void getMaxCachedEntries() {
		assertEquals(LRUCache.DEFAULT_MAX_CACHED_ENTRIES, new LRUCache<>().getMaxCachedEntries());
		assertEquals(LRUCache.DEFAULT_MAX_CACHED_ENTRIES, new LRUCache<>(Suppliers.ofInstance(0)).getMaxCachedEntries());
		assertEquals(LRUCache.DEFAULT_MAX_CACHED_ENTRIES, new LRUCache<>(Suppliers.ofInstance(-1)).getMaxCachedEntries());
		assertEquals(1, new LRUCache<>(Suppliers.ofInstance(1)).getMaxCachedEntries());
	}

	@Test
	public void testReplace() {
		LRUCache<String, String> cache = new LRUCache<>();
		cache.put("a", "a1");
		assertEquals("a1", cache.put("a", "a2"));
	}
	
	@Test(expected=NullPointerException.class)
	public void testPutNull() {
		LRUCache<String, String> cache = new LRUCache<>();
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
		LRUCache<String, String> cache = new LRUCache<>();
		cache.put("a", "a1");
		assertEquals("a1", cache.remove("a"));
	}
	
	@Test
	public void testRemoveNotExisiting() {
		LRUCache<String, String> cache = new LRUCache<>();
		cache.put("a", "a1");
		assertNull(cache.remove("b"));
	}

	@Test
	public void testClear() {
		LRUCache<String, String> cache = new LRUCache<>();
		cache.put("a", "a1");
		assertEquals("a1", cache.get("a"));
		cache.clear();
		assertNull(cache.get("a"));
	}
	
}