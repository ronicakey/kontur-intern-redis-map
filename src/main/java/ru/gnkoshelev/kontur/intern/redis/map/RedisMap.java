package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

import java.time.Duration;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Redis hash based implementation of the {@code Map} interface. This
 * implementation only accepts String objects for keys and values,
 * and permits {@code null} values and {@code null} key.
 * This class makes no guarantees as to the order of the map; in particular,
 * it does not guarantee that the order will remain constant over time.
 *
 * <p>Most operations, including aggregate operations such as {@code putAll}
 * and {@code clear}, are either atomic or employ optimistic locking.
 * Iterators return elements reflecting the state of the Redis hash at some point
 * at/since the creation of the iterator. They do <em>not</em> throw
 * {@link java.util.ConcurrentModificationException ConcurrentModificationException}.
 *
 * <p>RedisMap class has the no-argument constructor that instantiates
 * a new RedisMap object linked to a new Redis hash object.
 * If it is required to link an additional RedisMap to an existing Redis hash,
 * there are two parameterized constructors that accept either Redis hash key or id.
 * Both create a new Redis hash object with the specified key/id
 * if no existing object with such a key/id is found.
 *
 *  @author  Veronica Kazarina
 */
public class RedisMap implements Map<String, String> {
    /*
    * Implementation notes.
    *
    * Redis is used to atomically generate unique id's for RedisMap instances
    * created with the default constructor. The counter is reset to zero when
    * the limit of Integer.MAX_VALUE is reached. It is assumed that by this time
    * hash objects created earlier have already been evicted by Redis due to
    * memory shortage, lack of activity, etc.
    *
    * Since Redis does not store nulls, String tokens are used instead.
    *
    * To overcome the inability of Redis to store empty objects, every RedisMap
    * instance contains an "empty entry", that has no effect on map operations
    * other than to make it visible in Redis even when it has no actual entries.
    *
    * Lifetime of linked Redis hash objects is controlled through their "time to live"
    * that is reset to default value at fixed intervals. Redis automatically disposes
    * of the hashes when their associated RedisMap objects are no longer accessible.
    *
    * All iterators are based on RedisIterator class that uses HSCAN command
    * to iterate over hash keys. The implementation was tested on RedisMap objects
    * with different sizes, and it turned out that hashes sized below 512 fields
    * were returned by Redis in one go. SCAN_COUNT parameter was only used
    * for instances exceeding 511 entries. That means that in case of a smaller map,
    * concurrent modifications while iterating produce no changes; whereas,
    * in case of a bigger map, only additions to/removals from yet unvisited hash bins
    * are visible.
    *
    * When a map is resized significantly (more than 4 times) during iteration,
    * duplicate entries may be introduced by Redis; moreover, some duplicate keys
    * may also change value. To address this issue, every key provided by
    * RedisIterator is checked in case it has been seen in previous iterations.
    */

    /* ---------------- Constants -------------- */

    /**
     * The prefix used for Redis key generation.
     */
    private static final String KEY_PREFIX = "redis-map";

    /**
     * The default pattern for Redis keys.
     * All user-supplied keys are tested against this pattern.
     */
    private static final String KEY_PATTERN = KEY_PREFIX + ":\\d+";

    /**
     * The expiration time of the linked Redis hash object (in seconds).
     * The time is reset to this value when Redis object is accessed.
     */
    private static final int KEY_TTL = 30;

    /**
     * The key for a Redis object that stores the number
     * of RedisMap instances created with the default constructor.
     */
    private static final String OBJECT_COUNTER = KEY_PREFIX + "-counter";

    /**
     * The expiration time of the counter object (in seconds).
     * The time is reset to this value when the counter is accessed
     * (when the default constructor is used to initialise a RedisMap object).
     */
    private static final int COUNTER_TTL = 5 * 60;

    /**
     * Maximum RedisMap id value.
     */
    private static final int MAX_COUNT = Integer.MAX_VALUE;

    /**
     * The String that substitutes null keys and values in Redis.
     */
    private static final String NULL_TOKEN = "NULL_TOKEN";

    /**
     * The String used to create an "empty entry".
     * Putting this entry to an empty RedisMap makes it visible in Redis,
     * while the map's isEmpty() method still returns true.
     */
    private static final String EMPTY_FIELD_TOKEN = "EMPTY_FIELD_TOKEN";

    /**
     * The number of elements returned by Redis at every scan call.
     * Used by RedisIterator class.
     */
    private static final int SCAN_COUNT = 100;

    /**
     * Connection pool.
     */
    private static final JedisPool POOL;

    //pool configuration parameters

    /**
     * The upper bound of connections managed by the pool.
     */
    //the set value assumes that the number of RedisMap instances equals 10
    private static final int MAX_TOTAL = 100;

    /**
     * The upper bound of connections that can be idle
     * without being immediately closed.
     */
    private static final int MAX_IDLE = MAX_TOTAL;

    /**
     * The minimum number of idle connections to maintain in the pool.
     */
    //each RedisMap instance needs at least 1 connection to update its expiration time
    private static final int MIN_IDLE = 10;

    /**
     * The parameter that determines whether connections are tested
     * before they are borrowed from the pool.
     */
    private static final boolean TEST_ON_BORROW = true;

    /**
     * The parameter that determines whether connections are tested
     * before they are returned to the pool.
     */
    private static final boolean TEST_ON_RETURN = true;

    /**
     * The parameter that determines whether to block threads
     * when no connections are available.
     */
    private static final boolean BLOCK_WHEN_EXHAUSTED = true;

    /**
     * The maximum amount of time (in milliseconds) threads should be blocked
     * before throwing an exception when the pool is exhausted.
     * If set to -1, waits indefinitely.
     * Should be strictly less than KEY_TTL value to prevent Redis hash object
     * removal due to delayed expiration time update.
     */
    private static final int MAX_WAIT = 5;

    /**
     * The minimum amount of time a connection may remain idle
     * before it is eligible for eviction if MIN_IDLE instances are available.
     */
    private static final int SOFT_IDLE_TIME = 30;

    /**
     * The delay used by RedisMap instance's scheduler.
     */
    private static final int UPDATE_INTERVAL = KEY_TTL - MAX_WAIT;

    /* ---------------- Fields -------------- */

    /**
     * Service used to update Redis hash expiration time.
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * The key of the Redis hash object this map is linked to.
     */
    private final String redisKey;

    //views
    private transient Set<String> keySet;
    private transient Collection<String> values;
    private transient Set<Entry<String, String>> entrySet;

    /* ---------------- Public operations -------------- */

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(MAX_TOTAL);
        config.setMaxIdle(MAX_IDLE);
        config.setMinIdle(MIN_IDLE);
        config.setBlockWhenExhausted(BLOCK_WHEN_EXHAUSTED);
        config.setMaxWaitMillis(Duration.ofSeconds(MAX_WAIT).toMillis());
        config.setTestOnBorrow(TEST_ON_BORROW);
        config.setTestOnReturn(TEST_ON_RETURN);
        config.setSoftMinEvictableIdleTimeMillis(SOFT_IDLE_TIME);
        POOL = new JedisPool(config);
    }

    /**
     * Constructs an empty {@code RedisMap} linked to a new Redis hash.
     */
    public RedisMap() {
        redisKey = generateKey();
        initialize();
        setScheduler();
    }

    /**
     * Constructs a {@code RedisMap} linked to a Redis hash stored at the key with the specified id.
     * The id should be strictly greater than zero and less than or equal to Integer.MAX_VALUE.
     * If the key with such an id does not exist, an empty {@code RedisMap} linked to the key is constructed.
     *
     * @param id the id of the key whose associated value is to be linked to this map
     * @throws IllegalArgumentException if the {@code id} is not a valid id or
     *      the type of value stored at key with this {@code id} is not hash
     */
    public RedisMap(long id) {
        redisKey = validateKey(id);
        initialize();
        setScheduler();
    }

    /**
     * Constructs a {@code RedisMap} linked to a Redis hash stored at the specified key.
     * The key should adhere to the default "redis-map:" + id format where id is
     * strictly greater than zero and less than or equal to Integer.MAX_VALUE.
     * If the key does not exist, an empty {@code RedisMap} linked to the key is constructed.
     *
     * @param key the key whose associated value is to be linked to this map
     * @throws NullPointerException if the specified {@code key} is null
     * @throws IllegalArgumentException if the {@code key} is not valid or
     *      the type of value stored at {@code key} is not hash
     */
    public RedisMap(String key) {
        redisKey = validateKey(key);
        initialize();
        setScheduler();
    }

    /**
     * Makes an empty {@code RedisMap} visible in Redis.
     */
    private void initialize() {
        try (Jedis jedis = POOL.getResource()) {
            jedis.hset(redisKey, EMPTY_FIELD_TOKEN, EMPTY_FIELD_TOKEN);
        }
    }

    /**
     * Sets the scheduler to update Redis hash expiration time at regular intervals.
     */
    private void setScheduler() {
        scheduler.scheduleWithFixedDelay(() -> {
            try (Jedis jedis = POOL.getResource()) {
                jedis.expire(redisKey, KEY_TTL);
            }}, 0, UPDATE_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        long size;
        return (size = getHashSize()) > Integer.MAX_VALUE ? Integer.MAX_VALUE :
                Math.toIntExact(size);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return getHashSize() == 0L;
    }

    /**
     * Implements RedisMap.size and related methods.
     * Assumes that the "empty entry" is in place.
     */
    private long getHashSize() {
        try (Jedis jedis = POOL.getResource()) {
            return Math.max(jedis.hlen(redisKey) - 1, 0);
        }
    }

    /**
     * Returns {@code true} if this map contains a mapping for the specified key.
     *
     * @param key The key whose presence in this map is to be tested
     * @return {@code true} if this map contains a mapping for the specified key.
     */
    @Override
    public boolean containsKey(Object key) {
        key = nullToToken(key);
        if (key instanceof String) {
            return containsHashKey((String) key);
        }
        return false;
    }

    /**
     * Implements RedisMap.containsKey and related methods.
     */
    private boolean containsHashKey(String key) {
        try (Jedis jedis = POOL.getResource()) {
            return jedis.hexists(redisKey, key);
        }
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the specified value.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if this map maps one or more keys to the specified value
     */
    @Override
    public boolean containsValue(Object value) {
        value = nullToToken(value);
        if (value instanceof String) {
            return containsHashValue((String) value);
        }
        return false;
    }

    /**
     * Implements RedisMap.containsValue and related methods.
     */
    private boolean containsHashValue(String value) {
        Iterator<List<Entry<String, String>>> iterator = new RedisIterator();
        while (iterator.hasNext()) {
            for (Entry<String, String> entry : iterator.next()) {
                if (entry.getValue().equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * {@code null} if this map contains no mapping for the key.
     * <p>A return value of {@code null} does not <i>necessarily</i> indicate
     * that the map contains no mapping for the key;
     * it's also possible that the map explicitly maps the key to {@code null}.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null
     *      if this map contains no mapping for the key
     */
    @Override
    public String get(Object key) {
        key = nullToToken(key);
        if (key instanceof String) {
            Entry<String, String> entry;
            return (entry = getHashField((String) key)) == null ? null :
                    tokenToNull(entry.getValue());
        }
        return null;
    }

    /**
     * Implements RedisMap.get and related methods.
     */
    private Entry<String, String> getHashField(String key) {
        try (Jedis jedis = POOL.getResource()) {
            String value = jedis.hget(redisKey, key);
            return value == null ? null : new AbstractMap.SimpleEntry<>(key, value);
        }
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with {@code key}, or {@code null}
     *      if there was no mapping for {@code key}.(A {@code null} return can also indicate
     *      that the map previously associated {@code null} with {@code key}.)
     */
    @Override
    public String put(String key, String value) {
        key = (String) nullToToken(key);
        value = (String) nullToToken(value);
        String output = setField(key, value, false);
        return tokenToNull(output);
    }

    /**
     * Implements RedisMap.put and related methods.
     */
    private String setField(String key, String value, boolean onlyIfAbsent) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                if (!onlyIfAbsent || (output == null || NULL_TOKEN.equals(output))) {
                    Transaction transaction = jedis.multi();
                    transaction.hset(redisKey, key, value);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                }
                jedis.unwatch();
                return output;
            }
        }
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key whose mapping is to be removed from the map
     * @return the previous value associated with {@code key}, or {@code null}
     * if there was no mapping for {@code key}.(A {@code null} return can also indicate
     * that the map previously associated {@code null} with {@code key}.)
     */
    @Override
    public String remove(Object key) {
        key = nullToToken(key);
        if (key instanceof String) {
            Entry<String, String> entry = removeField((String) key, null);
            return entry == null ? null : tokenToNull(entry.getValue());
        }
        return null;
    }

    /**
     * Implements RedisMap.remove and related methods.
     * Attempts to atomize remove operation.
     */
    private Entry<String, String> removeField(String key, String value) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                if (output != null) {
                    if (value == null || output.equals(value)) {
                        Transaction transaction = jedis.multi();
                        transaction.hdel(redisKey, key);
                        List<Object> results = transaction.exec();
                        if (results == null) {
                            continue;
                        }
                        return new AbstractMap.SimpleEntry<>(key, output);
                    }
                }
                jedis.unwatch();
                return null;
            }
        }
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     * These mappings will replace any mappings that this map had for
     * any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     * @throws NullPointerException if the specified map is null
     */
    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        if (m instanceof RedisMap) {
            if (redisKey.equals(((RedisMap) m).getRedisKey())) {
                return;
            }
        }
        Map<String, String> map = m.keySet().stream()
                .collect(Collectors.toMap(k -> (String) nullToToken(k),
                        k -> (String) nullToToken(m.get(k))));
        setAllFields(map);
    }

    /**
     * Implements RedisMap.putAll.
     */
    private void setAllFields(Map<String, String> map) {
        try (Jedis jedis = POOL.getResource()) {
            jedis.hset(redisKey, map);
        }
    }

    /**
     * Removes all of the mappings from this map.
     * The map will be empty after this call returns.
     */
    @Override
    public void clear() {
        clearHash();
    }

    /**
     * Implements RedisMap.clear and related methods.
     */
    private void clearHash() {
        try (Jedis jedis = POOL.getResource()) {
            Transaction transaction = jedis.multi();
            transaction.unlink(redisKey);
            transaction.hset(redisKey, EMPTY_FIELD_TOKEN, EMPTY_FIELD_TOKEN);
            transaction.expire(redisKey, KEY_TTL);
            transaction.exec();
        }
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
     * The set supports element removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove}, {@code removeAll}, {@code retainAll},
     * and {@code clear} operations.
     * It does not support the {@code add} or {@code addAll} operations.
     *
     * <p>The view's iterators are <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return a set view of the keys contained in this map
     */
    @Override
    public Set<String> keySet() {
        Set<String> ks;
        return (ks = keySet) == null ? (keySet = new KeySet()) : ks;
    }

    final class KeySet extends AbstractSet<String> {
        public final int size() {
            return RedisMap.this.size();
        }

        public boolean isEmpty() {
            return RedisMap.this.isEmpty();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<String> iterator() {
            return new KeyIterator();
        }

        public final boolean contains(Object key) {
            return RedisMap.this.containsKey(key);
        }

        public final boolean remove(Object key) {
            key = nullToToken(key);
            if (key instanceof String) {
                return RedisMap.this.removeField((String) key, null) != null;
            }
            return false;
        }

        public final boolean removeAll(Collection<?> collection) {
            Objects.requireNonNull(collection);
            return RedisMap.this.removeAll(getKeyList(collection, false));
        }

        public final boolean retainAll(Collection<?> collection) {
            Objects.requireNonNull(collection);
            return RedisMap.this.removeAll(getKeyList(collection, true));
        }

        private List<String> getKeyList(Collection<?> collection, boolean retain) {
            Iterator<String> iterator = iterator();
            List<String> keyList = new ArrayList<>(size());
            String key;
            while (iterator.hasNext()) {
                if (retain ^ collection.contains(key = iterator.next())) {
                    keyList.add((String) nullToToken(key));
                }
            }
            return keyList;
        }
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are reflected in the collection,
     * and vice-versa. The collection supports element removal,
     * which removes the corresponding mapping from the map, via the {@code Iterator.remove},
     * {@code Collection.remove}, {@code removeAll}, {@code retainAll} and {@code clear} operations.
     * It does not support the {@code add} or {@code addAll} operations.
     *
     * <p>The view's iterators are <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return a view of the values contained in this map
     */
    @Override
    public Collection<String> values() {
        Collection<String> vs;
        return (vs = values) == null ? (values = new Values()) : vs;
    }

    final class Values extends AbstractCollection<String> {
        public final int size() {
            return RedisMap.this.size();
        }

        public boolean isEmpty() {
            return RedisMap.this.isEmpty();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<String> iterator() {
            return new ValueIterator();
        }

        public final boolean contains(Object value) {
            return RedisMap.this.containsValue(value);
        }

        public final boolean removeAll(Collection<?> collection) {
            Objects.requireNonNull(collection);
            return RedisMap.this.removeAll(getKeyList(collection, false));
        }

        public final boolean retainAll(Collection<?> collection) {
            Objects.requireNonNull(collection);
            return RedisMap.this.removeAll(getKeyList(collection, true));
        }

        private List<String> getKeyList(Collection<?> collection, boolean retain) {
            Iterator<String> iterator = iterator();
            List<String> keyList = new ArrayList<>(size());
            while (iterator.hasNext()) {
                if (retain ^ collection.contains(iterator.next())) {
                    keyList.add((String) nullToToken(((ValueIterator)iterator).getCurrentKey()));
                }
            }
            return keyList;
        }

    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
     * The set supports element removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove}, {@code removeAll}, {@code retainAll} and
     * {@code clear} operations.
     * It does not support the {@code add} or {@code addAll} operations.
     *
     * <p>The view's iterators are <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return a set view of the mappings contained in this map
     */
    @Override
    public Set<Entry<String, String>> entrySet() {
        Set<Entry<String, String>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
    }

    final class EntrySet extends AbstractSet<Entry<String, String>> {
        public final int size() {
            return RedisMap.this.size();
        }

        public boolean isEmpty() {
            return RedisMap.this.isEmpty();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<Entry<String, String>> iterator() {
            return new EntryIterator();
        }

        public final boolean contains(Object object) {
            if (!(object instanceof Entry)) {
                return false;
            }
            Entry<?,?> entry = (Entry<?,?>) object;
            Object key = nullToToken(entry.getKey());
            if (key instanceof String) {
                Entry<? extends String, ? extends String> output = RedisMap.this.getHashField((String) key);
                if (output != null) {
                    output = tokenToEntry(output.getKey(), output.getValue());
                }
                return output != null && output.equals(entry);
            }
            return false;
        }

        public final boolean remove(Object object) {
            if (object instanceof Entry) {
                Entry<?,?> entry = (Entry<?,?>) object;
                Object key = nullToToken(entry.getKey());
                Object value = nullToToken(entry.getValue());
                if (key instanceof String) {
                    return RedisMap.this.removeField((String) key, (String) value) != null;
                }
            }
            return false;
        }

        public final boolean removeAll(Collection<?> collection) {
            Objects.requireNonNull(collection);
            return RedisMap.this.removeAll(getKeyList(collection, false));
        }

        public final boolean retainAll(Collection<?> collection) {
            Objects.requireNonNull(collection);
            return RedisMap.this.removeAll(getKeyList(collection, true));
        }

        private List<String> getKeyList(Collection<?> collection, boolean retain) {
            Iterator<Entry<String, String>> iterator = iterator();
            List<String> keyList = new ArrayList<>(size());
            Entry<String, String> entry;
            while (iterator.hasNext()) {
                if (retain ^ collection.contains(entry = iterator.next())) {
                    keyList.add((String) nullToToken(entry.getKey()));
                }
            }
            return keyList;
        }
    }

    /**
     * Used by the views' removeAll and retainAll methods.
     */
    private boolean removeAll(List<String> keyList) {
        if (keyList.isEmpty()) {
            return false;
        }
        String[] keyArray = new String[keyList.size()];
        return removeAllFields(keyList.toArray(keyArray)) != 0;
    }

    /**
     * Implements removeAll method.
     */
    private long removeAllFields(String[] keys) {
        try (Jedis jedis = POOL.getResource()) {
            return jedis.hdel(redisKey, keys);
        }
    }

    /**
     * Compares the specified object with this map for equality.
     * Returns {@code true} if the given object is a map with the same
     * mappings as this map. This operation may return misleading
     * results if either map is concurrently modified during execution
     * of this method.
     *
     * @param object object to be compared for equality with this map
     * @return {@code true} if the specified object is equal to this map
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Map)) {
            return false;
        }
        Map<?,?> other = (Map<?,?>) object;
        if (size() != other.size()) {
            return false;
        }
        if (other instanceof RedisMap) {
            if (redisKey.equals(((RedisMap) other).getRedisKey())) {
                return true;
            }
        }
        for (Entry<String, String> entry : entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                if (!(other.get(key) == null && other.containsKey(key))) {
                    return false;
                }
            } else {
                if (!value.equals(other.get(key))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the hash code value for this map.
     * The hash code of a map is defined to be the sum of the hash codes of each entry
     * in the map's {@code entrySet()} view.
     *
     * @return the hash code value for this map
     */
    @Override
    public int hashCode() {
        int hash = 0;
        for (Entry<String, String> entry : entrySet()) {
            hash += entry.hashCode();
        }
        return hash;
    }

    /**
     * Returns a string representation of this map.
     * The string representation consists of a list of key-value mappings
     * enclosed in braces ({@code "{}"}). Adjacent mappings are separated
     * by the characters {@code ", "} (comma and space).
     * Each entry is rendered as the key, an equals character ("{@code =}"),
     * and the associated element, where the toString method is used
     * to convert the key and element to strings.
     *
     * This implementation returns the string representation preceded
     * by the key of the linked Redis hash object.
     *
     * @return a string representation of this map
     */
    @Override
    public String toString() {
        return entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", redisKey + " {", "}"));
    }

    //JDK8 Map extension methods

    /**
     * Returns the value to which the specified key is mapped, or the
     * given default value if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if this map contains no mapping
     *                     for the given key
     * @return the mapping for the key, if present; else the default value
     */
    @Override
    public String getOrDefault(Object key, String defaultValue) {
        key = nullToToken(key);
        if (key instanceof String) {
            Entry<String, String> entry;
            return (entry = getHashField((String) key)) == null ? defaultValue :
                    tokenToNull(entry.getValue());
        }
        return defaultValue;
    }

    /**
     * {@inheritDoc}
     *
     * This method checks if the key is still present in the map
     * before performing the action.
     */
    @Override
    public void forEach(BiConsumer<? super String, ? super String> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        for (Entry<String, String> entry : entrySet()) {
            if (containsKey(entry.getKey())) {
                action.accept(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method replaces values for the keys that are
     * still present in the map at the time of replacement.
     */
    @Override
    public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
        if (function == null) {
            throw new NullPointerException();
        }
        for (Entry<String, String> entry : entrySet()) {
            String key = (String) nullToToken(entry.getKey());
            replaceFieldValue(key, function);
        }
    }

    /**
     * Implements RedisMap.replaceAll method.
     */
    private void replaceFieldValue(String key,
                                   BiFunction<? super String, ? super String, ? extends String> function) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                if (output != null) {
                    String value = function.apply(key, tokenToNull(output));
                    value = (String) nullToToken(value);
                    Transaction transaction = jedis.multi();
                    transaction.hset(redisKey, key, value);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                    return;
                }
                jedis.unwatch();
                return;
            }
        }
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        key = (String) nullToToken(key);
        oldValue = (String) nullToToken(oldValue);
        newValue = (String) nullToToken(newValue);
        return replaceFieldValue(key, oldValue, newValue) != null;
    }

    @Override
    public String replace(String key, String value) {
        key = (String) nullToToken(key);
        value = (String) nullToToken(value);
        Entry<String, String> output = replaceFieldValue(key, null, value);
        return output == null ? null : tokenToNull(output.getValue());
    }

    /**
     * Implements RedisMap.replace methods.
     */
    private Entry<String, String> replaceFieldValue(String key, String oldValue, String newValue) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                if (output != null) {
                    if (oldValue == null || output.equals(oldValue)) {
                        Transaction transaction = jedis.multi();
                        transaction.hset(redisKey, key, newValue);
                        List<Object> results = transaction.exec();
                        if (results == null) {
                            continue;
                        }
                        return new AbstractMap.SimpleEntry<>(key, output);
                    }
                }
                jedis.unwatch();
                return null;
            }
        }
    }

    @Override
    public String putIfAbsent(String key, String value) {
        key = (String) nullToToken(key);
        value = (String) nullToToken(value);
        String output = setField(key, value, true);
        return tokenToNull(output);
    }

    @Override
    public boolean remove(Object key, Object value) {
        key = nullToToken(key);
        value = nullToToken(value);
        if (key instanceof String && value instanceof String) {
            return removeField((String) key, (String) value) != null;
        }
        return false;
    }

    @Override
    public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
        if (mappingFunction == null) {
            throw new NullPointerException();
        }
        String value = mappingFunction.apply(key);
        key = (String) nullToToken(key);
        String output = resetFieldIfAbsent(key, value);
        return tokenToNull(output);
    }

    /**
     * Implements RedisMap.computeIfAbsent.
     */
    private String resetFieldIfAbsent(String key, String value) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                if (value != null)  {
                    if (output == null || NULL_TOKEN.equals(output)) {
                        Transaction transaction = jedis.multi();
                        transaction.hset(redisKey, key, value);
                        List<Object> results = transaction.exec();
                        if (results == null) {
                            continue;
                        }
                        return value;
                    }
                }
                jedis.unwatch();
                return output;
            }
        }
    }

    @Override
    public String computeIfPresent(String key,
                                   BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        if (remappingFunction == null) {
            throw new NullPointerException();
        }
        key = (String) nullToToken(key);
        String output = resetFieldIfPresent(key, remappingFunction);
        return tokenToNull(output);
    }

    /**
     * Implements RedisMap.computeIfPresent.
     */
    private String resetFieldIfPresent(String key,
                                       BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                if (output != null && !NULL_TOKEN.equals(output)) {
                    String value = remappingFunction.apply(key, output);
                    if (value != null) {
                        Transaction transaction = jedis.multi();
                        transaction.hset(redisKey, key, value);
                        List<Object> results = transaction.exec();
                        if (results == null) {
                            continue;
                        }
                        return value;
                    }
                    Transaction transaction = jedis.multi();
                    transaction.hdel(redisKey, key);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                    return null;
                }
                jedis.unwatch();
                return output;
            }
        }
    }

    @Override
    public String compute(String key,
                          BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        if (remappingFunction == null) {
            throw new NullPointerException();
        }
        key = (String) nullToToken(key);
        return resetField(key, remappingFunction);
    }

    /**
     * Implements RedisMap.compute.
     */
    private String resetField(String key,
                              BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                String value = remappingFunction.apply(key, tokenToNull(output));
                if (value != null) {
                    Transaction transaction = jedis.multi();
                    transaction.hset(redisKey, key, value);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                    return value;
                }
                if (output != null) {
                    Transaction transaction = jedis.multi();
                    transaction.hdel(redisKey, key);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                    return null;
                }
                jedis.unwatch();
                return null;
            }
        }
    }

    @Override
    public String merge(String key, String value,
                        BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        if (value == null || remappingFunction == null) {
            throw new NullPointerException();
        }
        key = (String) nullToToken(key);
        return mergeField(key, value, remappingFunction);
    }

    /**
     * Implements RedisMap.merge.
     */
    private String mergeField(String key, String value,
                              BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(redisKey);
                String output = jedis.hget(redisKey, key);
                String newValue;
                if (output == null || NULL_TOKEN.equals(output)) {
                    newValue = value;
                } else {
                    newValue = remappingFunction.apply(output, value);
                }
                if (newValue != null) {
                    Transaction transaction = jedis.multi();
                    transaction.hset(redisKey, key, newValue);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                    return newValue;
                }
                Transaction transaction = jedis.multi();
                transaction.hdel(redisKey, key);
                List<Object> results = transaction.exec();
                if (results == null) {
                    continue;
                }
                return null;
            }
        }
    }

    // RedisMap getters

    public String getRedisKey() {
        return redisKey;
    }

    public int getTimeToLive() {
        return KEY_TTL;
    }

    /* ---------------- Key management -------------- */

    private String generateKey() {
        long id;
        String key;
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                if ((id = jedis.incr(OBJECT_COUNTER)) > MAX_COUNT) { //atomic
                    resetCounter();
                    continue;
                }
                if (!jedis.exists(key = assembleKey(id))) { //maps initialized with user-defined id/key
                    jedis.expire(OBJECT_COUNTER, COUNTER_TTL);
                    return key;
                }
            }
        }
    }

    // Prevents multiple deletions by different threads
    private void resetCounter() {
        try (Jedis jedis = POOL.getResource()) {
            while (true) {
                jedis.watch(OBJECT_COUNTER);
                long counter = Long.parseLong(jedis.get(OBJECT_COUNTER));
                if (counter > MAX_COUNT) {
                    Transaction transaction = jedis.multi();
                    transaction.del(OBJECT_COUNTER);
                    List<Object> results = transaction.exec();
                    if (results == null) {
                        continue;
                    }
                }
                jedis.unwatch();
                return;
            }
        }
    }

    private String assembleKey(long id) {
        return String.format("%s:%d", KEY_PREFIX, id);
    }

    private String validateKey(String key) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (isValidKey(key) && isValidType(key)) {
            return key;
        }
        throw new IllegalArgumentException("Illegal key: " + key);
    }

    private String validateKey(long id) {
        if (isValidId(id)) {
            String key = assembleKey(id);
            if (isValidType(key)) {
                return key;
            }
        }
        throw new IllegalArgumentException("Illegal id: " + id);
    }

    private boolean isValidKey(String key) {
        if (Pattern.compile(KEY_PATTERN).matcher(key.trim()).matches()) {
            long id = Long.parseLong(key.substring(key.indexOf(':') + 1));
            return isValidId(id);
        }
        return false;
    }

    private boolean isValidId(long id) {
        return id > 0L && id <= MAX_COUNT;
    }

    private boolean isValidType(String key) {
        try (Jedis jedis = POOL.getResource()) {
            String type = jedis.type(key);
            return "none".equals(type) || "hash".equals(type);
        }
    }

    /* ---------------- Null conversion -------------- */

    private Object nullToToken(Object arg) {
        return arg == null ? NULL_TOKEN : arg;
    }

    private String tokenToNull(String arg) {
        return NULL_TOKEN.equals(arg) ? null : arg;
    }

    private Entry<String, String> tokenToEntry(String key, String value) {
        return new AbstractMap.SimpleEntry<>(tokenToNull(key), tokenToNull(value));
    }

    /* ---------------- Iterators -------------- */

    abstract class BaseIterator {
        Iterator<List<Entry<String, String>>> iterator;
        Queue<Entry<String, String>> queue;
        Set<String> keys;
        Entry<String, String> current;

        BaseIterator() {
            queue = new ArrayDeque<>();
            iterator = new RedisIterator();
            keys = new HashSet<>();
            while (queue.isEmpty() && iterator.hasNext()) {
                processEntries(iterator.next());
            }
        }

        public boolean hasNext() {
            return !queue.isEmpty();
        }

        public Entry<String, String> nextEntry() {
            if (queue.isEmpty()) {
                throw new NoSuchElementException();
            }
            current = queue.remove();
            while (queue.isEmpty() && iterator.hasNext()) {
                processEntries(iterator.next());
            }
            return current;
        }

        public void remove() {
            Entry<String, String> entry = current;
            if (entry == null) {
                throw new IllegalStateException();
            }
            current = null;
            RedisMap.this.removeField((String) nullToToken(entry.getKey()), null);
        }

        //helper method used by Values removeAll and retainAll methods
        protected String getCurrentKey() {
            return current.getKey();
        }

        private void processEntries(List<Entry<String, String>> entries) {
            entries.stream()
                    .filter(e -> !EMPTY_FIELD_TOKEN.equals(e.getKey()))
                    .filter(e -> keys.add(e.getKey())) //remove possible (k,v) duplicates
                    .map(e -> tokenToEntry(e.getKey(), e.getValue()))
                    .forEach(e -> queue.add(e));
        }
    }

    final class KeyIterator extends BaseIterator implements Iterator<String> {
        @Override
        public String next() {
            return nextEntry().getKey();
        }
    }

    final class ValueIterator extends BaseIterator implements Iterator<String> {
        @Override
        public String next() {
            return nextEntry().getValue();
        }
    }

    final class EntryIterator extends BaseIterator implements Iterator<Entry<String, String>> {
        @Override
        public Entry<String, String> next() {
            return nextEntry();
        }
    }

    final class RedisIterator implements Iterator<List<Entry<String, String>>> {
        private final ScanParams scanParams;
        private String cursor;

        public RedisIterator() {
            this.scanParams = new ScanParams().count(SCAN_COUNT);
        }

        @Override
        public boolean hasNext() {
            return !"0".equals(cursor);
        }

        @Override
        public List<Entry<String, String>> next() {
            try (Jedis jedis = POOL.getResource()) {
                if (cursor == null) {
                    cursor = "0";
                }
                ScanResult<Entry<String, String>> scanResult = jedis.hscan(redisKey, cursor, scanParams);
                cursor = scanResult.getCursor();
                return scanResult.getResult();
            }
        }
    }
}