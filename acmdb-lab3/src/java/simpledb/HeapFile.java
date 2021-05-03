package simpledb;

import javax.xml.crypto.Data;
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
            randomAccessFile.close();
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
        int pageNumber = page.getId().pageNumber();
        byte[] data = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.f, "rw");
        randomAccessFile.seek(pageNumber*BufferPool.getPageSize());
        randomAccessFile.write(data, 0, BufferPool.getPageSize());
        randomAccessFile.close();
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
        if (!this.td.equals(t.getTupleDesc()))
        {
            throw new DbException("not match td in HeapFile insertTuple");
        }
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++)
        {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage tmpPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            try{
                tmpPage.insertTuple(t);
                modifiedPages.add(tmpPage);
                return modifiedPages;
            } catch (DbException ignored)
            {

            }
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        modifiedPages.add(newPage);
        writePage(newPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if (!this.td.equals(t.getTupleDesc()))
        {
            throw new DbException("not match td in HeapFile deleteTuple");
        }
        ArrayList<Page> modifiedPages = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage tPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        tPage.deleteTuple(t);
        modifiedPages.add(tPage);
        return modifiedPages;
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
            while (!this.iterator.hasNext() && this.curPid < numPages() - 1)
            {
                this.curPid ++;
                HeapPageId pid = new HeapPageId(getId(), curPid);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                this.iterator = heapPage.iterator();
            }
            return this.iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
        {
            if (!hasNext()) throw new NoSuchElementException("tupleNext");
            if (this.iterator.hasNext()) return this.iterator.next();
            curPid ++;
            HeapPageId pid = new HeapPageId(getId(), curPid);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
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
            this.curPid = 0;
            this.iterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

