package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final LRUCache<PageId, Page> buffer;
    private final LockManager lockManager = new LockManager();
    private final ConcurrentHashMap<TransactionId, HashSet<PageId>> holds = new ConcurrentHashMap<>();

    private void addHold(TransactionId tid, PageId pid) {
        holds.computeIfAbsent(tid, k -> new HashSet<>());
        synchronized (holds.get(tid)) {
            holds.get(tid).add(pid);
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        buffer = new LRUCache<>(numPages, (pid, page) -> page.isDirty() == null);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        try {
            lockManager.acquireLock(pid, tid, perm);
        } catch (DeadlockException e) {
            String msg = tid.getId() + ":" + Database.getCatalog().getTableName(pid.getTableId()) + ":" + pid.getPageNumber();
            throw new TransactionAbortedException(msg);
        }
        addHold(tid, pid);
        if (!buffer.containsKey(pid)) {
            try {
                // the evicted page in no steal mode is unnecessarily to flush
                buffer.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
            } catch (IllegalStateException e) {
                throw new DbException("can not evict");
            }
        }
        return buffer.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        synchronized (holds.get(tid)) {
            holds.get(tid).remove(pid);
        }
        lockManager.releaseLock(pid, tid);
    }

    public void releaseAllLock(TransactionId tid) {
        synchronized (holds.get(tid)) {
            for (PageId pid : holds.get(tid)) {
                lockManager.releaseLock(pid, tid);
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        if (holds.containsKey(tid)) {
            synchronized (holds.get(tid)) {
                try {
                    flushPages(tid);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                releaseAllLock(tid);
                holds.get(tid).clear();
            }
        }
    }

    public void transactionAbort(TransactionId tid) {
        synchronized (holds.get(tid)) {
            for (PageId pid : holds.get(tid)) {
                removePage(pid);
            }
            releaseAllLock(tid);
            holds.get(tid).clear();
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            transactionComplete(tid);
        } else {
            transactionAbort(tid);
        }
    }

    private void markAndBuffer(List<Page> list, TransactionId tid) {
        for (Page page : list) {
            page.markDirty(true, tid);
            buffer.put(page.getId(), page);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        List<Page> marked = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        markAndBuffer(marked, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        int tableID = t.getRecordId().getPageId().getTableId();
        List<Page> marked = Database.getCatalog().getDatabaseFile(tableID).deleteTuple(tid, t);
        markAndBuffer(marked, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        buffer.forEach((pid, page) -> {
            try {
                flushPage(pid, page);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        if (!buffer.containsKey(pid)) {
            return;
        }
        Page page = buffer.get(pid);
        flushPage(pid, page);
        // use current page contents as the before-image for the next transaction that modifies this page.
        page.setBeforeImage();
    }

    private synchronized void flushPage(PageId pid, Page page) throws IOException {
        if (page.isDirty() != null) {
            // append an update record to the log, with a before-image and after-image.
            Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        synchronized (holds.get(tid)) {
            for (PageId pid : holds.get(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        PageId evicted = buffer.evict();
        if (evicted == null) {
            throw new DbException("can not evict");
        }
    }
}
