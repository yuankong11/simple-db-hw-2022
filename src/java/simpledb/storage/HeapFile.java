package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    File file;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        // may not unique
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            int number = pid.getPageNumber(), size = BufferPool.getPageSize();
            int offset = number * size;
            randomAccessFile.seek(offset);
            byte[] b = new byte[size];
            randomAccessFile.read(b, 0, size);
            return new HeapPage(new HeapPageId(pid), b);
        } catch (IOException e) {
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new DbFileIterator() {
            Iterator<Tuple> it;
            int pageNumber = 0;
            final int totalPage = numPages();
            final BufferPool bp = Database.getBufferPool();
            final Permissions perm = Permissions.READ_ONLY;

            private Iterator<Tuple> getIterator() {
                try {
                    HeapPage page = (HeapPage) bp.getPage(tid, new HeapPageId(getId(), pageNumber++), perm);
                    return page.iterator();
                } catch (Exception e) {
                    return null;
                }
            }

            private void updateIterator() {
                while (pageNumber < totalPage && (it == null || !it.hasNext())) {
                    it = getIterator();
                }
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                updateIterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it != null && it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Tuple t = it.next();
                if (!it.hasNext()) {
                    updateIterator();
                }
                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = null;
                pageNumber = 0;
                updateIterator();
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }

}

