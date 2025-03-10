package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

class ReadWriteLock<O> {
    enum LockState {
        SHARED, EXCLUSIVE
    }

    private LockState state = LockState.SHARED;
    private final HashSet<O> owners = new HashSet<>();
    private final Object locked;
    private final CycleDetection cycle;

    public ReadWriteLock(Object locked, CycleDetection cycle) {
        this.locked = locked;
        this.cycle = cycle;
    }

    private void wait(O owner) throws DeadlockException {
        try {
            if (cycle.addEdge(owner, this)) {
                cycle.removeEdge(owner, this);
                throw new DeadlockException();
            }
            wait();
            cycle.removeEdge(owner, this);
        } catch (InterruptedException ignored) {
        }
    }

    public synchronized void readLock(O owner) throws DeadlockException {
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

    public synchronized void writeLock(O owner) throws DeadlockException {
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
            try {
                wait(owner);
            } catch (DeadlockException e) {
                if (owners.contains(owner)) {
                    e.setUpgradeDeadlock();
                }
                throw e;
            }
        }
        cycle.addEdge(this, owner);
    }

    public synchronized void releaseLock(O owner) {
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

    public synchronized boolean holdLock(O owner) {
        return owners.contains(owner);
    }

    @Override
    public String toString() {
        return "Lock{" + state + ", " + owners + ", " + locked + '}';
    }
}

public class LockManager {
    private final ConcurrentHashMap<PageId, ReadWriteLock<TransactionId>> locks = new ConcurrentHashMap<>();
    private final CycleDetection cycleDetection = new CycleDetection();

    private void putIfAbsent(PageId pid) {
        // use computeIfAbsent to avoid needless object creation
        locks.computeIfAbsent(pid, k -> new ReadWriteLock<>(pid, cycleDetection));
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

    public void acquireLock(PageId pid, TransactionId tid, Permissions perm) throws DeadlockException {
        try {
            acquireLockOnce(pid, tid, perm);
        } catch (DeadlockException e) {
            if (e.isUpgradeDeadlock()) {
                // sleep and try again, maybe one will win, avoid retry all tx
                long avoid = new Random().nextInt(500);
                try {
                    Thread.sleep(avoid);
                } catch (InterruptedException ignored) {
                }
                acquireLockOnce(pid, tid, perm);
            }
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
