package ru.gnkoshelev.kontur.intern.redis.map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RedisMapTest {
    static Jedis jedis;

    @BeforeClass
    public static void setUp() {
        jedis = new Jedis();
        //jedis.flushAll();
    }

    @AfterClass
    public static void tearDown() {
        jedis.close();
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new RedisMap();

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        assertEquals("1", map1.get("one"));
        assertEquals(1, map1.size());
        assertEquals(2, map2.size());

        map1.put("one", "first");

        assertEquals("first", map1.get("one"));
        assertEquals(1, map1.size());

        assertTrue(map1.containsKey("one"));
        assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        assertEquals(2, keys2.size());
        assertTrue(keys2.contains("one"));
        assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        assertEquals(1, values1.size());
        assertTrue(values1.contains("first"));
    }

    @Test
    public void testGet_Put_Remove() {
        Map<String, String> map = new RedisMap();
        assertNull(map.get("key"));
        assertNull(map.put("key1", "value1"));
        assertEquals("value1", map.get("key1"));
        assertEquals("value1", map.put("key1", "value2"));
        assertNull(map.remove("key"));
        assertEquals("value2", map.remove("key1"));
        assertNull(map.get("key1"));

        map.put("1", "2");
        assertNull(map.get(1));
        assertNull(map.remove(1));

        map.put(null, "value1");
        map.put("key2", null);
        assertNull(map.get("key2"));
        assertNull(map.put("key2", "value2"));
        assertEquals("value1", map.remove(null));
    }

    @Test
    public void testContainsKey_ContainsValue() {
        Map<String, String> map = new RedisMap();
        assertFalse(map.containsKey("key"));
        assertFalse(map.containsValue("value"));

        map.put("key1", "value1");
        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsValue("value1"));
        assertFalse(map.containsKey("key2"));
        assertFalse(map.containsValue("value2"));

        map.put("1", "2");
        assertFalse(map.containsKey(1));
        assertFalse(map.containsValue(2));

        map.put(null, "value1");
        map.put("key2", null);
        assertTrue(map.containsKey(null));
        assertTrue(map.containsValue(null));
    }

    @Test
    public void testIsEmpty_Clear() {
        Map<String, String> map = new RedisMap();
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());

        map.put("key", "value");
        assertFalse(map.isEmpty());
        assertEquals(1, map.size());

        map.put(null, null);
        assertEquals(2, map.size());

        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void testPutAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new RedisMap();
        Map<String, String> map3 = new HashMap<>();

        map1.put("key1", "value1");
        map1.put("key2", "value2");

        map2.put("key1", null);
        map2.put(null, "value3");

        map3.put(null, "value4");

        map1.putAll(map2);
        assertNull(map1.get("key1"));
        assertEquals(3, map1.size());

        map1.putAll(map3);
        assertEquals("value4", map1.get(null));
        assertEquals(3, map1.size());
    }

    @Test
    public void testPutAll_NullArg() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().putAll(null);
    }

    @Test
    public void testEquals_HashCode() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Map<String, String> map3 = new HashMap<>();
        Map<String, String> map4 = new RedisMap();
        Map<String, String> map5 = new RedisMap();
        Map<Integer, Integer> map6 = new HashMap<>();

        assertEquals(map1, map6);

        map1.put("key1", null);
        map1.put(null, "value2");

        map2.put("key1", null);

        map3.put("key1", null);
        map3.put(null, "value2");

        map4.put("key2", null);
        map4.put(null, "value2");

        map5.put("key1", null);
        map5.put(null, "value1");

        map6.put(1, null);
        map6.put(null, 2);

        assertEquals(map1, map1);
        assertNotEquals(map1, map2);
        assertEquals(map1, map3);
        assertNotEquals(map1, map4);
        assertNotEquals(map1, map5);
        assertNotEquals(map1, map6);

        assertNotEquals(map1.hashCode(), map2.hashCode());
        assertEquals(map1.hashCode(), map3.hashCode());
        assertNotEquals(map1.hashCode(), map4.hashCode());
        assertNotEquals(map1.hashCode(), map5.hashCode());

        assertNotEquals(map1, null);
        assertNotEquals(map1, map1.entrySet());
    }

    @Test
    public void testKeySet_Size_IsEmpty() {
        Map<String, String> map = new RedisMap();
        Set<String> keys = map.keySet();

        assertTrue(keys.isEmpty());
        assertEquals(0, keys.size());

        map.put("key1", "value1");
        assertFalse(keys.isEmpty());
        assertEquals(1, keys.size());
        map.put("key1", "value2");
        assertEquals(1, keys.size());

        map.put(null, "value1");
        map.put("key2", null);
        assertEquals(3, keys.size());
    }

    @Test
    public void testValues_Size_IsEmpty() {
        Map<String, String> map = new RedisMap();
        Collection<String> values = map.values();

        assertTrue(values.isEmpty());
        assertEquals(0, values.size());

        map.put("key1", "value1");
        assertFalse(values.isEmpty());
        assertEquals(1, values.size());

        map.put(null, "value2");
        map.put("key1", "value2");
        map.put("key2", null);
        assertEquals(3, values.size());

        map.remove("key1");
        assertEquals(2, values.size());
    }

    @Test
    public void testEntrySet_Size_IsEmpty() {
        Map<String, String> map = new RedisMap();
        Set<Map.Entry<String, String>> entries = map.entrySet();

        assertTrue(entries.isEmpty());
        assertEquals(0, entries.size());

        map.put("key1", "value1");
        assertFalse(entries.isEmpty());
        assertEquals(1, entries.size());

        map.put(null, "value1");
        map.put("key1", "value2");
        map.put("key2", null);
        assertEquals(3, entries.size());

        map.remove("key1");
        assertEquals(2, entries.size());
    }

    @Test
    public void testKeySet_Values_EntrySet_Contains() {
        Map<String, String> map = new RedisMap();
        Set<String> keys = map.keySet();
        Collection<String> values = map.values();
        Set<Map.Entry<String, String>> entries = map.entrySet();

        map.put("key1", "value1");
        assertTrue(keys.contains("key1"));
        assertTrue(values.contains("value1"));
        assertTrue(entries.contains(new AbstractMap.SimpleEntry<>("key1", "value1")));

        assertFalse(keys.contains("key2"));
        assertFalse(values.contains("value2"));
        assertFalse(entries.contains(new AbstractMap.SimpleEntry<>("key1", "value2")));
        assertFalse(entries.contains(new AbstractMap.SimpleEntry<>("key2", "value1")));
        assertFalse(entries.contains(new AbstractMap.SimpleEntry<>(1, "value1")));
        assertFalse(entries.contains("key1"));

        map.put(null, "value1");
        map.put("key2", null);
        assertTrue(keys.contains(null));
        assertTrue(values.contains(null));
        assertTrue(entries.contains(new AbstractMap.SimpleEntry<>((String) null, "value1")));
        assertTrue(entries.contains(new AbstractMap.SimpleEntry<>("key2", (String) null)));
    }

    @Test
    public void testKeySet_Remove() {
        Map<String, String> map = new RedisMap();
        Set<String> keys = map.keySet();

        map.put(null, "value1");
        map.put("key1", null);

        assertFalse(keys.remove("key"));
        assertEquals(2, map.size());

        assertFalse(keys.remove(1));
        assertEquals(2, map.size());

        assertTrue(keys.remove(null));
        assertFalse(map.containsKey(null));

        assertTrue(keys.remove("key1"));
        assertFalse(map.containsKey("key1"));
    }

    @Test
    public void testValues_Remove() {
        Map<String, String> map = new RedisMap();
        Collection<String> values = map.values();

        map.put(null, "value1");
        map.put("key1", null);
        map.put("key2", "value1");

        assertFalse(values.remove("value"));
        assertEquals(3, map.size());

        assertFalse(values.remove(1));
        assertEquals(3, map.size());

        assertTrue(values.remove(null));
        assertFalse(map.containsValue(null));

        assertTrue(values.remove("value1"));
        assertTrue(values.contains("value1"));
        assertTrue(values.remove("value1"));
        assertFalse(values.contains("value1"));
    }

    @Test
    public void testEntrySet_Remove() {
        Map<String, String> map = new RedisMap();
        Set<Map.Entry<String, String>> entries = map.entrySet();

        map.put(null, "value1");
        map.put("key1", null);

        assertFalse(entries.remove(new AbstractMap.SimpleEntry<>("key1", "value1")));
        assertEquals(2, map.size());

        assertFalse(entries.remove("key"));
        assertEquals(2, map.size());

        assertFalse(entries.remove(new AbstractMap.SimpleEntry<>(1, 2)));
        assertEquals(2, map.size());

        assertTrue(entries.remove(new AbstractMap.SimpleEntry<>("key1", (String) null)));
        assertFalse(map.containsKey("key1"));
        assertFalse(map.containsValue(null));

        assertTrue(entries.remove(new AbstractMap.SimpleEntry<>((String) null, "value1")));
        assertFalse(map.containsKey(null));
        assertFalse(map.containsValue("value1"));
    }

    @Test
    public void testKeySet_Values_EntrySet_Clear() {
        Map<String, String> map = new RedisMap();
        Set<String> keys = map.keySet();
        Collection<String> values = map.values();
        Set<Map.Entry<String, String>> entries = map.entrySet();

        map.put("key1", "value1");
        keys.clear();
        assertTrue(map.isEmpty());

        map.put("key1", "value1");
        values.clear();
        assertTrue(map.isEmpty());

        map.put("key1", "value1");
        entries.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testKeySet_Iterator() {
        Map<String, String> map = new RedisMap();
        Iterator<String> keys = map.keySet().iterator();
        assertFalse(keys.hasNext());

        map.put("key1", "value1");
        map.put(null, "value2");
        map.put("key2", null);
        keys = map.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            assertTrue(map.containsKey(key));
            keys.remove();
            assertFalse(map.containsKey(key));
        }
        assertTrue(map.isEmpty());
        assertFalse(keys.hasNext());
    }

    @Test
    public void testValues_Iterator() {
        Map<String, String> map = new RedisMap();
        Iterator<String> values = map.values().iterator();
        assertFalse(values.hasNext());

        map.put("key1", "value1");
        map.put(null, "value2");
        map.put("key2", null);
        values = map.values().iterator();
        while (values.hasNext()) {
            String value = values.next();
            assertTrue(map.containsValue(value));
            values.remove();
            assertFalse(map.containsValue(value));
        }
        assertTrue(map.isEmpty());
        assertFalse(values.hasNext());
    }

    @Test
    public void testEntrySet_Iterator() {
        Map<String, String> map = new RedisMap();
        Iterator<Map.Entry<String, String>> entries = map.entrySet().iterator();
        assertFalse(entries.hasNext());

        map.put("key1", "value1");
        map.put(null, "value2");
        map.put("key2", null);
        entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, String> entry = entries.next();
            assertTrue(map.containsKey(entry.getKey()) && map.containsValue(entry.getValue()));
            entries.remove();
            assertFalse(map.containsKey(entry.getKey()));
        }
        assertTrue(map.isEmpty());
        assertFalse(entries.hasNext());
    }

    @Test
    public void testKeySet_EmptyIterator_Next() {
        exceptionRule.expect(NoSuchElementException.class);
        new RedisMap().keySet().iterator().next();
    }

    @Test
    public void testValues_EmptyIterator_Next() {
        exceptionRule.expect(NoSuchElementException.class);
        new RedisMap().values().iterator().next();
    }

    @Test
    public void testEntrySet_EmptyIterator_Next() {
        exceptionRule.expect(NoSuchElementException.class);
        new RedisMap().entrySet().iterator().next();
    }

    @Test
    public void testKeySet_EmptyIterator_Remove() {
        exceptionRule.expect(IllegalStateException.class);
        new RedisMap().keySet().iterator().remove();
    }

    @Test
    public void testValues_EmptyIterator_Remove() {
        exceptionRule.expect(IllegalStateException.class);
        new RedisMap().values().iterator().remove();
    }

    @Test
    public void testEntrySet_EmptyIterator_Remove() {
        exceptionRule.expect(IllegalStateException.class);
        new RedisMap().entrySet().iterator().remove();
    }

    @Test
    public void testKeySet_RetainAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Set<String> keys = map1.keySet();

        map1.put("key1", "value1");
        assertTrue(keys.retainAll(map2.keySet()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value1");
        map2.put("key2", null);
        assertFalse(keys.containsAll(map2.keySet()));
        assertTrue(keys.retainAll(map2.keySet()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);
        map2.put(null, "value2");
        map2.put("key2", null);
        map2.put("key1", null);
        assertFalse(keys.retainAll(map2.keySet()));
        assertTrue(keys.containsAll(map2.keySet()));
        assertEquals(3, map1.size());

        map2.remove("key1");
        assertTrue(keys.retainAll(map2.keySet()));
        assertEquals(2, map1.size());
    }

    @Test
    public void testValues_RetainAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Collection<String> values = map1.values();

        map1.put("key1", "value1");
        assertTrue(values.retainAll(map2.values()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value1");
        map2.put("key2", null);
        assertFalse(values.containsAll(map2.values()));
        assertTrue(values.retainAll(map2.values()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);
        map2.put(null, "value2");
        map2.put("key1", null);
        map2.put("key2", "value1");
        assertTrue(values.containsAll(map2.values()));
        assertFalse(values.retainAll(map2.values()));
        assertEquals(3, map1.size());

        map2.remove("key2");
        assertTrue(values.retainAll(map2.values()));
        assertEquals(2, map1.size());
    }

    @Test
    public void testEntrySet_RetainAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Set<Map.Entry<String, String>> entries = map1.entrySet();

        map1.put("key1", "value1");
        assertTrue(entries.retainAll(map2.entrySet()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value1");
        map2.put("key2", null);
        assertFalse(entries.containsAll(map2.entrySet()));
        assertTrue(entries.retainAll(map2.entrySet()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);
        map2.put("key1", "value1");
        map2.put(null, "value2");
        map2.put("key2", null);
        assertTrue(entries.containsAll(map2.entrySet()));
        assertFalse(entries.retainAll(map2.entrySet()));
        assertEquals(3, map1.size());

        map1.put("key3", "value1");
        assertTrue(entries.retainAll(map2.entrySet()));
        assertEquals(3, map1.size());
    }

    @Test
    public void testKeySet_RemoveAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Set<String> keys = map1.keySet();

        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);
        assertFalse(keys.removeAll(map2.keySet()));
        assertEquals(3, map1.size());

        map2.put("key1", "value1");
        assertTrue(keys.removeAll(map2.keySet()));
        assertEquals(2, map1.size());

        map2.put(null, "value1");
        map2.put("key2", "value2");
        assertTrue(keys.removeAll(map2.keySet()));
        assertTrue(map1.isEmpty());

        map1.put("key3", "value2");
        map1.put("key4", null);
        map1.put("key5", null);
        assertFalse(keys.removeAll(map2.keySet()));
        assertEquals(3, map1.size());
    }

    @Test
    public void testValues_RemoveAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Collection<String> values = map1.values();

        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);
        assertFalse(values.removeAll(map2.values()));
        assertEquals(3, map1.size());

        map2.put("key1", "value1");
        assertTrue(values.removeAll(map2.values()));
        assertEquals(2, map1.size());

        map2.put(null, "value2");
        map2.put("key3", null);
        assertTrue(values.removeAll(map2.values()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value3");
        map1.put("key3", "value4");
        map1.put(null, "value5");
        assertFalse(values.removeAll(map2.values()));
        assertEquals(3, map1.size());
    }

    @Test
    public void testEntrySet_RemoveAll() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new HashMap<>();
        Set<Map.Entry<String, String>> entries = map1.entrySet();

        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);
        assertFalse(entries.removeAll(map2.entrySet()));
        assertEquals(3, map1.size());

        map2.put("key1", "value1");
        assertTrue(entries.removeAll(map2.entrySet()));
        assertEquals(2, map1.size());

        map2.put(null, "value2");
        map2.put("key2", null);
        assertTrue(entries.removeAll(map2.entrySet()));
        assertTrue(map1.isEmpty());

        map1.put("key1", "value2");
        map1.put("key2", "value3");
        map1.put(null, "value51");
        assertFalse(entries.removeAll(map2.entrySet()));
        assertEquals(3, map1.size());
    }

    @Test
    public void testIterator_AddElements_SmallSet() {
        long id = 9825L;
        Map<String, String> map = new RedisMap(id);
        for (int i = 0; i < 300; i++) {
            map.put("key" + i, "value" + i);
        }
        assertEquals(300, map.size());
        Iterator<Map.Entry<String, String >> iterator = map.entrySet().iterator();

        List<String> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            String key = iterator.next().getKey();
            keys.add(key);
            if ("key150".equals(key)) {
                for (int i = 100; i < 1300; i++) {
                    jedis.hset("redis-map:" + id, "key" + i, "value" + (i + 1));
                }
                assertEquals(1300, map.size());
            }
        }
        assertEquals(keys.size(), new HashSet<>(keys).size());
        assertEquals(300, keys.size());
    }

    @Test
    public void testIterator_AddElements_BigSet() {
        long id = 3687L;
        Map<String, String> map = new RedisMap(id);
        for (int i = 0; i < 600; i++) {
            map.put("key" + i, "value" + i);
        }
        assertEquals(600, map.size());
        Iterator<Map.Entry<String, String >> iterator = map.entrySet().iterator();

        List<String> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            String key = iterator.next().getKey();
            keys.add(key);
            if ("key300".equals(key)) {
                for (int i = 100; i < 2500; i++) {
                    jedis.hset("redis-map:" + id, "key" + i, "value" + (i + 1));
                }
                assertEquals(2500, map.size());
            }
        }
        assertEquals(keys.size(), new HashSet<>(keys).size());
        assertTrue(keys.size() >= 600 && keys.size() <= 2500);
    }

    @Test
    public void testIterator_RemoveElements_SmallSet() {
        long id = 1295L;
        Map<String, String> map = new RedisMap(id);
        for (int i = 0; i < 400; i++) {
            map.put("key" + i, "value" + i);
        }
        assertEquals(400, map.size());
        Iterator<Map.Entry<String, String >> iterator = map.entrySet().iterator();

        List<String> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            String key = iterator.next().getKey();
            keys.add(key);
            if ("key200".equals(key)) {
                for (int i = 40; i < 360; i++) {
                    jedis.hdel("redis-map:" + id, "key" + i);
                }
                assertEquals(80, map.size());
            }
        }
        assertEquals(400, keys.size());
    }

    @Test
    public void testIterator_RemoveElements_BigSet() {
        long id = 2255L;
        Map<String, String> map = new RedisMap(id);
        for (int i = 0; i < 2000; i++) {
            map.put("key" + i, "value" + i);
        }
        assertEquals(2000, map.size());
        Iterator<Map.Entry<String, String >> iterator = map.entrySet().iterator();

        List<String> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            String key = iterator.next().getKey();
            keys.add(key);
            if ("key1000".equals(key)) {
                for (int i = 200; i < 1800; i++) {
                    jedis.hdel("redis-map:" + id, "key" + i);
                }
                assertEquals(400, map.size());
            }
        }
        assertEquals(keys.size(), new HashSet<>(keys).size());
        assertTrue(keys.size() <= 2000 && keys.size() >= 400);
    }

    @Test
    public void testGetOrDefault() {
        Map<String, String> map = new RedisMap();
        String defaultValue = "default";
        assertEquals(defaultValue, map.getOrDefault("key1", defaultValue));

        map.put("key1", "value1");
        map.put(null, "value2");
        map.put("key2", null);

        assertEquals("value1", map.getOrDefault("key1", defaultValue));
        assertEquals("value2", map.getOrDefault(null, defaultValue));
        assertNull(map.getOrDefault("key2", defaultValue));
        assertNull(map.getOrDefault("key3", null));

        assertEquals(defaultValue, map.getOrDefault(1, defaultValue));
    }

    @Test
    public void testForEach() {
        Map<String, String> map1 = new RedisMap();
        map1.put("key1", "value1");
        map1.put(null, "value2");
        map1.put("key2", null);

        Map<String, String> map2 = new RedisMap();
        map1.forEach(map2::put);
        assertEquals(map1, map2);

        map1.forEach((k, v) -> map2.remove(k));
        assertTrue(map2.isEmpty());
    }

    @Test
    public void testForEach_NullAction() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().forEach(null);
    }

    @Test
    public void testReplaceAll() {
        Map<String, String> map = new RedisMap();
        map.put("key1", "value1");
        map.put(null, "value2");
        map.put("key2", null);

        map.replaceAll((k, v) -> String.format("replaced %s", v));
        map.forEach((k, v) -> assertTrue(v.contains("replaced")));

        map.replaceAll((k, v) -> null);
        map.forEach((k, v) -> assertNull(v));
    }

    @Test
    public void testReplaceAll_NullFunction() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().replaceAll(null);
    }

    @Test
    public void testPutIfAbsent() {
        Map<String, String> map = new RedisMap();
        assertNull(map.putIfAbsent("key1", "value1"));
        assertEquals("value1", map.get("key1"));

        assertNull(map.putIfAbsent("key2", null));
        assertNull(map.get("key2"));

        assertNull(map.putIfAbsent(null, "value2"));
        assertEquals("value2", map.get(null));

        assertNull(map.putIfAbsent("key2", "value3"));
        assertEquals("value3", map.get("key2"));

        assertEquals("value3", map.putIfAbsent("key2", "value4"));
        assertEquals("value3", map.get("key2"));
    }

    @Test
    public void testRemoveKeyValue() {
        Map<String, String> map = new RedisMap();
        assertFalse(map.remove("key", "value"));

        map.put(null, "value1");
        map.put("key1", null);
        map.put("key2", "value1");

        assertTrue(map.remove(null, "value1"));
        assertTrue(map.remove("key1", null));
        assertFalse(map.remove("key2", "value2"));

        map.put("1", "2");
        assertFalse(map.remove(1, 2));
        assertFalse(map.remove(1, "2"));
        assertFalse(map.remove("1", 2));
    }

    @Test
    public void testReplaceOldValue() {
        Map<String, String> map = new RedisMap();
        assertFalse(map.replace("key", "value1", "value2"));
        assertTrue(map.isEmpty());

        map.put(null, "value1");
        map.put("key1", null);
        map.put("key2", "value1");
        assertTrue(map.replace(null, "value1", "value2"));
        assertEquals("value2", map.get(null));
        assertTrue(map.replace("key1", null, "value3"));
        assertEquals("value3", map.get("key1"));
        assertFalse(map.replace("key2", "value2", "value4"));
        assertEquals("value1", map.get("key2"));
    }

    @Test
    public void testReplaceValue() {
        Map<String, String> map = new RedisMap();
        assertNull(map.replace("key", "value1"));
        assertTrue(map.isEmpty());

        map.put(null, "value1");
        map.put("key1", null);
        assertEquals("value1", map.replace(null, "value2"));
        assertEquals("value2", map.get(null));
        assertNull(map.replace("key1", "value3"));
        assertEquals("value3", map.get("key1"));
    }

    @Test
    public void testComputeIfAbsent() {
        Map<String, String> map = new RedisMap();
        assertEquals("value1", map.computeIfAbsent("key1", k -> "value1"));
        assertEquals("value1", map.get("key1"));

        map.put("key2", null);
        assertEquals("value2", map.computeIfAbsent("key2", k -> "value2"));
        assertEquals("value2", map.get("key2"));

        assertEquals("value3", map.computeIfAbsent(null, k -> "value3"));
        assertEquals("value3", map.get(null));

        assertEquals("value1", map.computeIfAbsent("key1", k -> "value4"));
        assertEquals("value1", map.get("key1"));

        assertNull(map.computeIfAbsent("key3", k -> null));
        assertFalse(map.containsKey("key3"));
    }

    @Test
    public void testComputeIfAbsent_NullFunction() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().computeIfAbsent("key", null);
    }

    @Test
    public void testComputeIfPresent() {
        Map<String, String> map = new RedisMap();
        assertNull(map.computeIfPresent("key1", (k, v) -> "value1"));
        assertFalse(map.containsKey("key1"));

        map.put("key2", null);
        assertNull(map.computeIfPresent("key2", (k, v) -> "value2"));
        assertNull(map.get("key2"));

        map.put(null, "value3");
        assertEquals("value4", map.computeIfPresent(null, (k, v) -> "value4"));
        assertEquals("value4", map.get(null));

        map.put("key1", "value1");
        assertNull(map.computeIfPresent("key1", (k, v) -> null));
        assertFalse(map.containsKey("key1"));
    }

    @Test
    public void testComputeIfPresent_NullFunction() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().computeIfPresent("key", null);
    }

    @Test
    public void testCompute() {
        Map<String, String> map = new RedisMap();
        assertEquals("value1", map.compute("key1", (k, v) -> "value1"));
        assertEquals("value1", map.get("key1"));

        map.put("key2", null);
        assertNull(map.compute("key2", (k, v) -> null));
        assertFalse(map.containsKey("key2"));

        assertEquals("value2", map.compute("key2", (k, v) -> "value2"));
        assertEquals("value2", map.get("key2"));

        map.put(null, "value3");
        assertEquals("value4", map.compute(null, (k, v) -> "value4"));
        assertEquals("value4", map.get(null));

        assertNull(map.compute("key1", (k, v) -> null));
        assertFalse(map.containsKey("key1"));

        assertNull(map.compute("key3", (k, v) -> null));
        assertFalse(map.containsKey("key3"));
    }

    @Test
    public void testCompute_NullFunction() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().compute("key", null);
    }

    @Test
    public void testMerge() {
        Map<String, String> map = new RedisMap();
        assertEquals("value", map.merge("key1", "value", String::concat));
        assertEquals("value", map.get("key1"));

        assertEquals("value1", map.merge("key1", "1", String::concat));
        assertEquals("value1", map.get("key1"));

        map.put("key2", null);
        assertEquals("value2", map.merge("key2", "value2", String::concat));
        assertEquals("value2", map.get("key2"));

        map.put(null, "value");
        assertEquals("value3", map.merge(null, "3", String::concat));
        assertEquals("value3", map.get(null));

        assertNull(map.merge("key1", "value3", (v1, v2) -> null));
        assertFalse(map.containsKey("key1"));
    }

    @Test
    public void testMerge_NullValue() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().merge("key", null, String::concat);
    }

    @Test
    public void testMerge_NullFunction() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap().merge("key", "value", null);
    }

    @Test
    public void testConstructors_ExistingKey_GetRedisKey() {
        Map<String, String> map1 = new RedisMap();
        String key = ((RedisMap) map1).getRedisKey();
        assertTrue(jedis.exists(key));

        map1.put("key1", "value1");
        map1.put("key2", "value2");
        Map<String, String> map2 = new RedisMap(key);
        assertEquals(map1, map2);
        assertNotSame(map1, map2);
        assertEquals(key, ((RedisMap) map2).getRedisKey());
        map2.putAll(map1);
        assertEquals(2, map2.size());

        long id = Long.parseLong(key.substring(key.indexOf(':') + 1));
        Map<String, String> map3 = new RedisMap(id);
        assertEquals(map1, map3);
        assertNotSame(map1, map3);
        assertEquals(key, ((RedisMap) map3).getRedisKey());

        map2.remove("key1");
        assertEquals(1, map1.size());
        assertEquals(1, map3.size());

        map3.clear();
        assertTrue(map1.isEmpty());
        assertTrue(map2.isEmpty());
    }

    @Test
    public void testConstructor_StringArg_NonExistingKey() {
        String key = "redis-map:2567";
        assertFalse(jedis.exists(key));

        Map<String, String> map1 = new RedisMap(key);
        assertTrue(jedis.exists(key));

        Map<String, String> map2 = new RedisMap(key);
        assertEquals(map1, map2);
        assertNotSame(map1, map2);
        assertEquals(key, ((RedisMap)map1).getRedisKey());
        assertEquals(key, ((RedisMap)map2).getRedisKey());
    }

    @Test
    public void testConstructor_NumericArg_NonExistingKey() {
        long id = 1234L;
        String key = String.format("redis-map:%d", id);
        assertFalse(jedis.exists(key));

        Map<String, String> map1 = new RedisMap(id);
        assertTrue(jedis.exists(key));
        map1.put("key1", "value1");
        assertEquals("value1", jedis.hget(key, "key1"));

        jedis.hset(key, "key1", "value2");
        assertEquals("value2", map1.get("key1"));
    }

    @Test
    public void testConstructor_IllegalLongArg_Negative() {
        long id = -1L;
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Illegal id: " + id);
        new RedisMap(id);
    }

    @Test
    public void testConstructor_IllegalLongArg_Max() {
        long id = Integer.MAX_VALUE + 1L;
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Illegal id: " + id);
        new RedisMap(id);
    }

    @Test
    public void testConstructor_IllegalStringArg() {
        String key = "some-key";
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Illegal key: " + key);
        new RedisMap(key);
    }

    @Test
    public void testConstructor_IllegalStringArg_Zero() {
        String key = "redis-map:0";
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Illegal key: " + key);
        new RedisMap(key);
    }

    @Test
    public void testConstructor_IllegalStringArg_Max() {
        String key = "redis-map:" + Long.MAX_VALUE;
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Illegal key: " + key);
        new RedisMap(key);
    }

    @Test
    public void testConstructor_NullStringArg() {
        exceptionRule.expect(NullPointerException.class);
        new RedisMap(null);
    }

    @Test
    public void testConstructor_NotHashType() {
        exceptionRule.expect(IllegalArgumentException.class);
        jedis.setex("key", 5, "value");
        new RedisMap("key");
    }

    @Test
    public void testMapExpire() throws InterruptedException {
        Map<String, String> map = new RedisMap();
        String key = ((RedisMap) map).getRedisKey();
        int timeToLive = ((RedisMap) map).getTimeToLive();
        int idleTime = timeToLive + 1;

        map.put("key", "value");
        assertFalse(map.isEmpty());

        TimeUnit.SECONDS.sleep(idleTime);
        assertFalse(jedis.exists(key));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testMapExpire_WithUpdates() throws InterruptedException {
        Map<String, String> map = new RedisMap();
        String key = ((RedisMap) map).getRedisKey();
        int timeToLive = ((RedisMap) map).getTimeToLive();
        int idleTime = timeToLive / 2 + 1;

        map.put("key1", "value1");

        TimeUnit.SECONDS.sleep(idleTime);
        assertEquals(1, map.size());

        TimeUnit.SECONDS.sleep(idleTime);
        assertTrue(map.containsKey("key1"));

        TimeUnit.SECONDS.sleep(idleTime);
        assertTrue(map.containsValue("value1"));

        TimeUnit.SECONDS.sleep(idleTime);
        assertNull(map.put("key2", "value1"));

        TimeUnit.SECONDS.sleep(idleTime);
        assertEquals("value1", map.get("key1"));

        TimeUnit.SECONDS.sleep(idleTime);
        assertEquals("value1", map.remove("key1"));

        TimeUnit.SECONDS.sleep(idleTime);
        map.clear();
        assertTrue(jedis.exists(key));

        TimeUnit.SECONDS.sleep(timeToLive + 1);
        assertFalse(jedis.exists(key));
    }
}