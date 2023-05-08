package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

class ReadWriteLock<O> {
    enum LockState {
        SHARED, EXCLUSIVE
    }

    LockState state = LockState.SHARED;
    HashSet<O> owners = new HashSet<>();

    private void waitNoEx() {
        try {
            wait();
        } catch (InterruptedException ignored) {
        }
    }

    public void readLock(O owner) {
        synchronized (this) {
            while (true) {
                if (owners.contains(owner)) {
                    return;
                }
                if (state == LockState.SHARED) {
                    owners.add(owner);
                    return;
                }
                waitNoEx();
            }
        }
    }

    public void writeLock(O owner) {
        synchronized (this) {
            while (true) {
                if (owners.isEmpty()) {
                    state = LockState.EXCLUSIVE;
                    owners.add(owner);
                    return;
                }
                if (owners.size() == 1 && owners.contains(owner)) {
                    state = LockState.EXCLUSIVE;
                    return;
                }
                waitNoEx();
            }
        }
    }

    public void releaseLock(O owner) {
        synchronized (this) {
            if (owners.contains(owner)) {
                owners.remove(owner);
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
    ConcurrentHashMap<TransactionId, HashSet<PageId>> holds = new ConcurrentHashMap<>();

    private void addHold(TransactionId tid, PageId pid) {
        holds.putIfAbsent(tid, new HashSet<>());
        synchronized (holds.get(tid)) {
            holds.get(tid).add(pid);
        }
    }

    private void putIfAbsent(PageId pid) {
        locks.putIfAbsent(pid, new ReadWriteLock<>());
    }

    public void acquireReadLock(PageId pid, TransactionId tid) {
        putIfAbsent(pid);
        locks.get(pid).readLock(tid);
        addHold(tid, pid);
    }

    public void acquireWriteLock(PageId pid, TransactionId tid) {
        putIfAbsent(pid);
        locks.get(pid).writeLock(tid);
        addHold(tid, pid);
    }

    public void acquireLock(PageId pid, TransactionId tid, Permissions perm) {
        if (perm == Permissions.READ_ONLY) {
            acquireReadLock(pid, tid);
        } else {
            acquireWriteLock(pid, tid);
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

    public void releaseAll(TransactionId tid) {
        if (holds.containsKey(tid)) {
            synchronized (holds.get(tid)) {
                holds.get(tid).forEach(pid -> releaseLock(pid, tid));
                holds.get(tid).clear();
            }
        }
    }
}
