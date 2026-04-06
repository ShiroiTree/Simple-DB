package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     *  TODO: add a dead lock detection
     */
    private static class Lock {
        private final PageId pid;
        private final Set<TransactionId> sharedLocks;
        private TransactionId exclusiveLock;

        public Lock(PageId pid) {
            this.pid = pid;
            this.sharedLocks = new HashSet<>();
            this.exclusiveLock = null;
        }

        public synchronized boolean acquireLock(TransactionId tid, Permissions perm) {
            if (perm == Permissions.READ_ONLY) {
                return acquireShared(tid);
            } else {
                return acquireExclusive(tid);
            }
        }

        private boolean acquireShared(TransactionId tid) {
            while (exclusiveLock != null && exclusiveLock.equals(tid)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            sharedLocks.add(tid);
            return true;
        }

        private boolean acquireExclusive(TransactionId tid) {
            while (exclusiveLock != null && !exclusiveLock.equals(tid)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            while (!sharedLocks.isEmpty() &&
                    !(sharedLocks.size() == 1 && sharedLocks.contains(tid))) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            // 清除自己的共享锁（锁升级）
            sharedLocks.remove(tid);
            exclusiveLock = tid;
            return true;
        }

        public synchronized void release(TransactionId tid) {
            boolean wasHolding = sharedLocks.remove(tid);

            if (exclusiveLock != null && exclusiveLock.equals(tid)) {
                exclusiveLock = null;
                wasHolding = true;
            }
            if (wasHolding) {
                notifyAll();
            }
        }

        public synchronized boolean holdsLock(TransactionId tid) {
            return sharedLocks.contains(tid) ||
                    (exclusiveLock != null && exclusiveLock.equals(tid));
        }

        public synchronized boolean hasNoHolders() {
            return sharedLocks.isEmpty() && exclusiveLock == null;
        }
    }

    /**
     *  Only use locker to access the Lock class to keep lock table unique.
     */
    private static final Map<PageId, Lock> lockMap = new ConcurrentHashMap<>();

    private final Integer numPages;
    private final Map<PageId, Page> pageCache;

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
        if (pageCache.size() >= numPages) {
            evictPage();
        }

        Page page = pageCache.get(pid);
        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            if (dbFile == null) {
                throw new DbException("No DbFile for table " + pid.getTableId());
            }
            page = dbFile.readPage(pid);
            pageCache.put(pid, page);
        }

        Lock lock = lockMap.computeIfAbsent(pid, Lock::new);
        boolean ifAcquired = lock.acquireLock(tid, perm);
        if (!ifAcquired) {
            throw new TransactionAbortedException();
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
        Lock lock = lockMap.get(pid);
        if (lock != null) {
            lock.release(tid);
        }
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
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
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
        }
        for (PageId pid : new ArrayList<>(lockMap.keySet())) {
            unsafeReleasePage(tid, pid);
        }
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
    private synchronized  void flushPage(PageId pid) throws IOException {
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

        List<PageId> pageIds = new ArrayList<>(pageCache.keySet());
        for (int attempt = 0; attempt < pageIds.size() * 2; attempt++) {
            int randomIndex = random.nextInt(pageIds.size());
            PageId victimId = pageIds.get(randomIndex);
            Page victimPage = pageCache.get(victimId);
            if (victimPage.isDirty() == null) {
                discardPage(victimId);
                pageCache.remove(victimId);
                return;
            }
        }
        throw new DbException("All pages in buffer pool are dirty!");
    }


}
