package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try{
            Page page = null;
            byte[] buffer = new byte[BufferPool.getPageSize()];
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.f,"r");
            randomAccessFile.seek(pid.pageNumber() * BufferPool.getPageSize());
            randomAccessFile.read(buffer, 0, BufferPool.getPageSize());
            page = new HeapPage((HeapPageId) pid, buffer);
            return page;
        } catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator{
        private TransactionId tid;
        private int curPid;
        private Iterator<Tuple> iterator;
        private boolean isClosed;

        public HeapFileIterator(TransactionId tid)
        {
            this.tid = tid;
            this.isClosed = true;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException
        {
            this.curPid = 0;
            HeapPageId pageId = new HeapPageId(getId(), curPid);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            this.iterator = heapPage.iterator();
            this.isClosed = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException
        {
            if (this.isClosed) return false;
            if (this.iterator.hasNext()) return true;
            return this.curPid < numPages() - 1;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
        {
            if (!hasNext()) throw new NoSuchElementException("tupleNext");
            if (this.iterator.hasNext()) return this.iterator.next();
            curPid ++;
            HeapPageId pageId = new HeapPageId(getId(), curPid);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            this.iterator = heapPage.iterator();
            return this.iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException
        {
            open();
        }

        @Override
        public void close()
        {
            this.isClosed = true;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

