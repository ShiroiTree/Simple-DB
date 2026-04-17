package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import simpledb.transaction.PageLock;
import simpledb.transaction.LockManager;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final Integer numPages;
    private final Map<PageId, Page> pageCache;

    private final LockManager lockManager = new LockManager();

    private final Random random = new Random();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if (tid == null || pid == null || perm == null) {
            throw new DbException("Invalid arguments to getPage");
        }

        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while (true) {
            try {
                if (lockManager.acquireLock(tid, pid, perm)) {
                    break;
                }
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new TransactionAbortedException();
            }
        }

        Page page = pageCache.get(pid);
        if (page == null && pageCache.size() >= numPages) {
            evictPage();
        }

        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            if (dbFile == null) {
                throw new DbException("No DbFile for table " + pid.getTableId());
            }
            page = dbFile.readPage(pid);
            pageCache.put(pid, page);
        }

        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
                PageId pid = entry.getKey();
                Page page = entry.getValue();
                if (page.isDirty() == tid) {
                    int tableId = pid.getTableId();
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                    Page cleanPage = dbFile.readPage(pid);
                    pageCache.put(pid, cleanPage);
                }
            }
            System.out.println("read from disk");
        }
        lockManager.releaseAllLock(tid);
    }

    private void updateBufferPool(List<Page> pageList, TransactionId tid) throws DbException {
        for (Page page : pageList) {
            page.markDirty(true, tid);
            if (pageCache.size() == numPages) {
                evictPage();
            }
            pageCache.put(page.getId(), page);
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
        updateBufferPool(dbFile.insertTuple(tid, t), tid);
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
        updateBufferPool(dbFile.deleteTuple(tid, t), tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
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
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page flush = pageCache.get(pid);
        int tableId = flush.getId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        TransactionId dirtier = flush.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, flush.getBeforeImage(), flush);
            Database.getLogFile().force();
        }
        dbFile.writePage(flush);
        flush.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            page.setBeforeImage();
            if (page.isDirty() == tid) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        if (pageCache.isEmpty()) {
            return;
        }

        for (Page page : pageCache.values()) {
            if (page.isDirty() == null) {
                discardPage(page.getId());
                pageCache.remove(page.getId());
                return;
            }
        }
        throw new DbException("All pages in buffer pool are dirty!");
    }


}
