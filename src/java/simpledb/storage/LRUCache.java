package simpledb.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

class LRUNode<V> {
    V val;
    LRUNode<V> prev, next;

    public LRUNode(V val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return "Node{" +
                "val=" + (val == null ? "null" : val) +
                ", prev=" + (prev.val == null ? "null" : prev.val) +
                ", next=" + (next.val == null ? "null" : next.val) +
                '}';
    }
}

class LRUList<V> {
    private final LRUNode<V> head = new LRUNode<>(null), tail = new LRUNode<>(null);

    public LRUList() {
        head.next = tail;
        tail.prev = head;
    }

    void addFirst(LRUNode<V> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    void addLast(LRUNode<V> node) {
        node.next = tail;
        node.prev = tail.prev;
        tail.prev.next = node;
        tail.prev = node;
    }

    LRUNode<V> addFirst(V v) {
        LRUNode<V> node = new LRUNode<>(v);
        addFirst(node);
        return node;
    }

    LRUNode<V> addLast(V v) {
        LRUNode<V> node = new LRUNode<>(v);
        addLast(node);
        return node;
    }

    boolean isEmpty() {
        return head.next == tail;
    }

    void remove(LRUNode<V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    LRUNode<V> removeFirst() {
        if (isEmpty()) {
            return null;
        }
        LRUNode<V> node = head.next;
        remove(node);
        return node;
    }

    LRUNode<V> removeLast() {
        if (isEmpty()) {
            return null;
        }
        LRUNode<V> node = tail.prev;
        remove(node);
        return node;
    }

    void moveToFirst(LRUNode<V> node) {
        if (head.next == node) {
            return;
        }
        remove(node);
        addFirst(node);
    }


    void moveToLast(LRUNode<V> node) {
        if (tail.prev == node) {
            return;
        }
        remove(node);
        addLast(node);
    }

    Iterator<V> iteratorFromFirst() {
        return new Iterator<V>() {
            LRUNode<V> cur = head;

            @Override
            public boolean hasNext() {
                return cur.next != tail;
            }

            @Override
            public V next() {
                cur = cur.next;
                return cur.val;
            }
        };
    }

    Iterator<V> iteratorFromLast() {
        return new Iterator<V>() {
            LRUNode<V> cur = tail;

            @Override
            public boolean hasNext() {
                return cur.prev != head;
            }

            @Override
            public V next() {
                cur = cur.prev;
                return cur.val;
            }
        };
    }

    void clear() {
        head.next = tail;
        tail.prev = head;
    }

    void print() {
        LRUNode<V> cur = head.next;
        while (cur != tail) {
            System.out.println(cur);
            cur = cur.next;
        }
    }
}

public class LRUCache<K, V> {
    static class Pair<K, V> {
        K k;
        V v;

        public Pair(K k, V v) {
            this.k = k;
            this.v = v;
        }
    }

    private final HashMap<K, LRUNode<Pair<K, V>>> nodes = new HashMap<>();
    private final LRUList<Pair<K, V>> list = new LRUList<>();
    private final int capacity;
    private final BiPredicate<K, V> predicate;
    private int size = 0;

    public LRUCache(int capacity, BiPredicate<K, V> predicate) {
        this.capacity = capacity;
        this.predicate = predicate;
    }

    public synchronized V get(K key) {
        if (nodes.containsKey(key)) {
            LRUNode<Pair<K, V>> node = nodes.get(key);
            list.moveToFirst(node);
            return node.val.v;
        } else {
            return null;
        }
    }

    public synchronized K put(K key, V value) throws IllegalStateException {
        if (nodes.containsKey(key)) {
            nodes.get(key).val.v = value;
            list.moveToFirst(nodes.get(key));
            return null;
        } else {
            K removed = null;
            boolean failed = false;
            if (size == capacity) {
                removed = evict();
                if (removed == null) {
                    failed = true;
                }
            }
            LRUNode<Pair<K, V>> node = list.addFirst(new Pair<>(key, value));
            nodes.put(key, node);
            size++;
            if (failed) {
                throw new IllegalStateException("can not evict");
            }
            return removed;
        }
    }

    public synchronized boolean containsKey(K key) {
        return nodes.containsKey(key);
    }

    public synchronized void remove(K key) {
        if (containsKey(key)) {
            LRUNode<Pair<K, V>> n = nodes.remove(key);
            list.remove(n);
            size--;
        }
    }

    public synchronized void clear() {
        nodes.clear();
        list.clear();
        size = 0;
    }

    public synchronized void forEach(BiConsumer<K, V> consumer) {
        Iterator<Pair<K, V>> it = list.iteratorFromFirst();
        while (it.hasNext()) {
            Pair<K, V> p = it.next();
            consumer.accept(p.k, p.v);
        }
    }

    public synchronized K evict() {
        Iterator<Pair<K, V>> it = list.iteratorFromLast();
        K removed = null;
        while (it.hasNext()) {
            Pair<K, V> p = it.next();
            if (predicate.test(p.k, p.v)) {
                removed = p.k;
                break;
            }
        }
        if (removed != null) {
            remove(removed);
        }
        return removed;
    }
}
