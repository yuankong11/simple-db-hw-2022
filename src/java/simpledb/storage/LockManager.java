package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

class ReadWriteLock<O> {
    public static final CycleDetection cycle = new CycleDetection();

    enum LockState {
        SHARED, EXCLUSIVE
    }

    LockState state = LockState.SHARED;
    HashSet<O> owners = new HashSet<>();

    private void wait(O o) throws DeadlockException {
        try {
            cycle.addEdge(o, this);
            HashSet<Object> path = cycle.getCycle();
            if (!path.isEmpty()) {
                throw new DeadlockException(path);
            }
            wait();
            cycle.removeEdge(o, this);
        } catch (InterruptedException ignored) {
        }
    }

    public void readLock(O owner) throws DeadlockException {
        synchronized (this) {
            while (true) {
                if (owners.contains(owner)) {
                    break;
                }
                if (state == LockState.SHARED) {
                    owners.add(owner);
                    break;
                }
                wait(owner);
            }
            cycle.addEdge(this, owner);
        }
    }

    public void writeLock(O owner) throws DeadlockException {
        synchronized (this) {
            while (true) {
                if (owners.isEmpty()) {
                    state = LockState.EXCLUSIVE;
                    owners.add(owner);
                    break;
                }
                if (owners.size() == 1 && owners.contains(owner)) {
                    state = LockState.EXCLUSIVE;
                    break;
                }
                wait(owner);
            }
            cycle.addEdge(this, owner);
        }
    }

    public void releaseLock(O owner) {
        synchronized (this) {
            if (owners.contains(owner)) {
                owners.remove(owner);
                cycle.removeEdge(this, owner);
                if (state == LockState.EXCLUSIVE) {
                    state = LockState.SHARED;
                    notify();
                } else {
                    if (owners.isEmpty()) {
                        notify();
                    }
                }
            }
        }
    }

    public boolean holdLock(O owner) {
        synchronized (this) {
            return owners.contains(owner);
        }
    }
}

public class LockManager {
    ConcurrentHashMap<PageId, ReadWriteLock<TransactionId>> locks = new ConcurrentHashMap<>();

    private void putIfAbsent(PageId pid) {
        // use computeIfAbsent to avoid needless object creation
        locks.computeIfAbsent(pid, k -> new ReadWriteLock<>());
    }

    public void acquireReadLock(PageId pid, TransactionId tid) throws DeadlockException {
        putIfAbsent(pid);
        locks.get(pid).readLock(tid);
    }

    public void acquireWriteLock(PageId pid, TransactionId tid) throws DeadlockException {
        putIfAbsent(pid);
        locks.get(pid).writeLock(tid);
    }

    public void acquireLockOnce(PageId pid, TransactionId tid, Permissions perm) throws DeadlockException {
        if (perm == Permissions.READ_ONLY) {
            acquireReadLock(pid, tid);
        } else {
            acquireWriteLock(pid, tid);
        }
    }

    private boolean inDeadlock(HashSet<Object> path, TransactionId tid) {
        for (Object o : path) {
            if (tid.equals(o)) {
                return true;
            }
        }
        return false;
    }

    public void acquireLock(PageId pid, TransactionId tid, Permissions perm) throws DeadlockException {
        try {
            acquireLockOnce(pid, tid, perm);
        } catch (DeadlockException e) {
            // sleep and try again, maybe one will win, avoid retry all tx
            if (inDeadlock(e.path, tid)) {
                long avoid = new Random().nextInt(500);
                try {
                    Thread.sleep(avoid);
                } catch (InterruptedException ignored) {
                }
            }
            acquireLockOnce(pid, tid, perm);
        }
    }

    public void releaseLock(PageId pid, TransactionId tid) {
        if (locks.containsKey(pid)) {
            locks.get(pid).releaseLock(tid);
        }
    }

    public boolean holdLock(PageId pid, TransactionId tid) {
        if (locks.containsKey(pid)) {
            return locks.get(pid).holdLock(tid);
        }
        return false;
    }
}
