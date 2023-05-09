package simpledb.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

class GraphNode {
    Object value;
    int count;
    HashSet<GraphNode> next = new HashSet<>();

    public GraphNode(Object value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphNode graphNode = (GraphNode) o;
        return Objects.equals(value, graphNode.value) && Objects.equals(next, graphNode.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}

public class CycleDetection {
    HashMap<Object, GraphNode> nodes = new HashMap<>();

    public synchronized GraphNode getNode(Object value) {
        if (nodes.containsKey(value)) {
            return nodes.get(value);
        }
        GraphNode node = new GraphNode(value);
        nodes.put(value, node);
        return node;
    }

    public synchronized void addEdge(Object src, Object dst) {
        addEdge(getNode(src), getNode(dst));
    }

    public synchronized void addEdge(GraphNode src, GraphNode dst) {
        src.next.add(dst);
        src.count++;
        dst.count++;
    }

    public synchronized void removeEdge(Object src, Object dst) {
        removeEdge(getNode(src), getNode(dst));
    }

    public synchronized void removeEdge(GraphNode src, GraphNode dst) {
        src.next.remove(dst);
        src.count--;
        dst.count--;
        if (src.count == 0) {
            removeNode(src);
        }
        if (dst.count == 0) {
            removeNode(dst);
        }
    }

    public synchronized void removeNode(GraphNode node) {
        nodes.remove(node);
    }

    private boolean DFS(GraphNode cur, HashSet<GraphNode> distinct, HashMap<GraphNode, Boolean> result, HashSet<Object> path) {
        if (distinct.contains(cur)) {
            if (path != null) {
                for (GraphNode n : distinct) {
                    path.add(n.value);
                }
            }
            result.put(cur, Boolean.TRUE);
            return true;
        }
        if (result.containsKey(cur)) {
            return result.get(cur);
        }
        distinct.add(cur);
        for (GraphNode n : cur.next) {
            if (DFS(n, distinct, result, path)) {
                result.put(cur, Boolean.TRUE);
                break;
            }
        }
        distinct.remove(cur);
        result.putIfAbsent(cur, Boolean.FALSE);
        return result.get(cur);
    }

    public synchronized HashSet<Object> getCycle() {
        HashSet<GraphNode> distinct = new HashSet<>();
        HashSet<Object> path = new HashSet<>();
        HashMap<GraphNode, Boolean> result = new HashMap<>();
        for (GraphNode node : nodes.values()) {
            if (DFS(node, distinct, result, path)) {
                return path;
            }
        }
        return path;
    }
}
