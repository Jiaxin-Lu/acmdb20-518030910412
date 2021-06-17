package simpledb;


import java.io.*;

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
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxNumPages;
    private Page[] pageBuffer;
    private boolean[] pageBufferUsed;
    private HashMap<PageId, Integer> pageIdHashMap;
    private LinkedList<PageId> LRUList;

    private class Dependency
    {
        public ConcurrentHashMap<TransactionId, Set<TransactionId>> tidToEdge = new ConcurrentHashMap<>();
        public Set<TransactionId> vis = Collections.synchronizedSet(new HashSet<>());

        synchronized void modifyEdges(TransactionId tid, PageId pid)
        {
            tidToEdge.putIfAbsent(tid, new HashSet<>());
            Set<TransactionId> edges = tidToEdge.get(tid);
            edges.clear();
            if (pid == null) return;

            Set <TransactionId> pidToTid;
            synchronized (pidToLock.get(pid))
            {
                pidToTid = pidToLock.get(pid).relatedTid();
            }
            edges.addAll(pidToTid);
        }

        boolean DFS(TransactionId tid, TransactionId fa)
        {
            vis.add(tid);
            Set<TransactionId> edges = tidToEdge.get(tid);
            if (edges == null) return false;
            boolean flag = false;
            for (TransactionId ne : edges)
            {
                if (ne.equals(fa)) return true;
                if (!vis.contains(ne))
                {
                    flag = flag || DFS(ne, fa);
                }
            }
            return flag;
        }

        synchronized boolean isDeadLocked(TransactionId tid)
        {
            vis.clear();
            return DFS(tid, tid);
        }
    }

    private ConcurrentHashMap<PageId, PageLock> pidToLock;
    private ConcurrentHashMap<TransactionId, Set<PageId>> tidToPid;
    private Dependency dependency;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxNumPages = numPages;
        this.pageBuffer = new Page[numPages];
        this.pageBufferUsed = new boolean[numPages];
        this.pageIdHashMap = new HashMap<>();
        for (int i = 0; i < this.pageBufferUsed.length; i++)
        {
            pageBufferUsed[i] = false;
        }
        this.LRUList = new LinkedList<>();
        pidToLock = new ConcurrentHashMap<>();
        tidToPid = new ConcurrentHashMap<>();
        dependency = new Dependency();
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
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        pidToLock.putIfAbsent(pid, new PageLock(pid));
        boolean success;
        synchronized (pidToLock.get(pid))
        {
            success = pidToLock.get(pid).addLock(perm, tid);
        }
        while (!success)
        {
            dependency.modifyEdges(tid, pid);
            if (dependency.isDeadLocked(tid))
            {
                throw new TransactionAbortedException();
            }
            synchronized (pidToLock.get(pid))
            {
                success = pidToLock.get(pid).addLock(perm, tid);
            }
        }
        dependency.modifyEdges(tid, null);
        tidToPid.putIfAbsent(tid, new HashSet<>());
        tidToPid.get(tid).add(pid);

        synchronized (this)
        {
            if (pageIdHashMap.containsKey(pid))
            {
                LRUList.remove(pid);
                LRUList.addLast(pid);
                return pageBuffer[pageIdHashMap.get(pid)];
            } else
            {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                int index = pageBuffer.length;
                for (int i = 0; i < pageBuffer.length; i++)
                {
                    if (!pageBufferUsed[i])
                    {
                        index = i;
                        break;
                    }
                }
                if (index >= pageBuffer.length)
                {
                    this.evictPage();
                    for (int i = 0; i < pageBuffer.length; i++)
                    {
                        if (!pageBufferUsed[i])
                        {
                            index = i;
                            break;
                        }
                    }
                }
                page.setBeforeImage();
                pageBuffer[index] = page;
                pageBufferUsed[index] = true;
                pageIdHashMap.put(pid, index);
                LRUList.addLast(pid);
                return page;
            }
        }
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
    public  void releasePage(TransactionId tid, PageId pid) {
        synchronized (pidToLock.get(pid))
        {
            pidToLock.get(pid).releaseLock(tid);
        }
        tidToPid.get(tid).remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        synchronized (pidToLock.get(p))
        {
            return pidToLock.get(p).isHolding(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        Set<PageId> lockPages = tidToPid.get(tid);
        tidToPid.remove(tid);
        if (lockPages == null)
        {
            return;
        }
        for (PageId pid : lockPages)
        {
            if (pageIdHashMap.containsKey(pid))
            {
                Page page = pageBuffer[pageIdHashMap.get(pid)];
                if (pidToLock.get(pid).isExclusive())
                {
                    if (commit)
                    {
                        if (page.isDirty() != null)
                        {
                            this.flushPage(pid);
                            page.markDirty(false, null);
                            page.setBeforeImage();
                        }
                    } else {
                        assert page.getBeforeImage() != null;
                        pageBuffer[pageIdHashMap.get(pid)] = page.getBeforeImage();
                    }
                }
            }
            synchronized (pidToLock.get(pid))
            {
                pidToLock.get(pid).releaseLock(tid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = dbFile.insertTuple(tid, t);
        synchronized (this)
        {
            for (Page dirtyPage : dirtyPages)
            {
                PageId pid = dirtyPage.getId();
                if (this.pageIdHashMap.containsKey(pid))
                {
                    LRUList.remove(pid);
                    LRUList.addLast(pid);
                    pageBuffer[pageIdHashMap.get(pid)] = dirtyPage;
                } else {
                    int index = pageBuffer.length;
                    for (int i = 0; i < pageBuffer.length; i++)
                    {
                        if (!pageBufferUsed[i])
                        {
                            index = i;
                            break;
                        }
                    }
                    while (index >= pageBuffer.length)
                    {
                        this.evictPage();
                        for (int i = 0; i < pageBuffer.length; i++)
                        {
                            if (!pageBufferUsed[i])
                            {
                                index = i;
                                break;
                            }
                        }
                    }
                    pageBuffer[index] = dirtyPage;
                    pageBufferUsed[index] = true;
                    pageIdHashMap.put(pid, index);
                    LRUList.addLast(pid);
                    dirtyPage.setBeforeImage();
                }
                dirtyPage.markDirty(true, tid);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        synchronized (this)
        {
            for (Page dirtyPage : dirtyPages)
            {
                PageId pid = dirtyPage.getId();
                if (this.pageIdHashMap.containsKey(pid))
                {
                    LRUList.remove(pid);
                    LRUList.addLast(pid);
                    pageBuffer[pageIdHashMap.get(pid)] = dirtyPage;
                } else {
                    int index = pageBuffer.length;
                    for (int i = 0; i < pageBuffer.length; i++)
                    {
                        if (!pageBufferUsed[i])
                        {
                            index = i;
                            break;
                        }
                    }
                    while (index >= pageBuffer.length)
                    {
                        this.evictPage();
                        for (int i = 0; i < pageBuffer.length; i++)
                        {
                            if (!pageBufferUsed[i])
                            {
                                index = i;
                                break;
                            }
                        }
                    }
                    pageBuffer[index] = dirtyPage;
                    pageBufferUsed[index] = true;
                    pageIdHashMap.put(pid, index);
                    LRUList.addLast(pid);
                    dirtyPage.setBeforeImage();
                }
                dirtyPage.markDirty(true, tid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : pageIdHashMap.keySet())
        {
            this.flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (pageIdHashMap.containsKey(pid))
        {
            int arrayIndex = pageIdHashMap.get(pid);
            pageBufferUsed[arrayIndex] = true;
            pageIdHashMap.remove(pid);
            LRUList.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        if (pageIdHashMap.containsKey(pid))
        {
            int arrayIndex = pageIdHashMap.get(pid);
            Page page = pageBuffer[arrayIndex];
            if (page.isDirty() == null) return;
            page.markDirty(false, null);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
//        Set<PageId> pids = tidToPid.get(tid);
//        if (pids != null)
//        {
//            for (PageId pid : pids)
//            {
//                if (!holdsLock(tid, pid)) return;
//            }
//            for (PageId pid : pids)
//            {
//                if (pidToLock.get(pid).isExclusiveTid(tid))
//                {
//                    flushPage(pid);
//                }
//            }
//        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        Iterator<PageId> iterator = LRUList.iterator();
        PageId pid = null;
        while (iterator.hasNext())
        {
            PageId tmpPid = iterator.next();
            Page page = pageBuffer[pageIdHashMap.get(tmpPid)];
            if (page.isDirty() == null)
            {
                pid = tmpPid;
                break;
            }
        }
        if (pid == null)
        {
            throw new DbException("all pages are dirty in evict page!");
        }
        int arrayIndex = pageIdHashMap.get(pid);
        try{
            this.flushPage(pid);
        }catch (IOException e)
        {
            e.printStackTrace();
            System.err.println("IOException in evictPage");
        }
        pageBufferUsed[arrayIndex] = false;
        pageIdHashMap.remove(pid);
        this.LRUList.remove(pid);
    }

}
