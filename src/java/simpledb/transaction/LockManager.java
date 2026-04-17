package simpledb.transaction;

import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class LockManager {
    private final Map<PageId, Map<TransactionId, PageLock>> pageLockMap;

    public LockManager() {
        pageLockMap = new ConcurrentHashMap<>();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions)
        throws TransactionAbortedException, InterruptedException {
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pid);
        if (lockMap == null || lockMap.isEmpty()) {     // 该页面无锁，直接加锁
            PageLock pageLock = new PageLock(tid, permissions);
            lockMap = new ConcurrentHashMap<>();
            lockMap.put(tid, pageLock);
            pageLockMap.put(pid, lockMap);
            return true;
        }

        PageLock pageLock = lockMap.get(tid);
        if (pageLock != null) {     // 该事物在该页面上有锁
            if (permissions == Permissions.READ_ONLY) {
                return true;
            }
            if (permissions == Permissions.READ_WRITE) {
                if (lockMap.size() > 1) {   // 其他事务也对该页面有锁，此时进行锁升级有死锁风险
                    throw new TransactionAbortedException();
                }
                if (lockMap.size() == 1 && pageLock.getPermissions() == Permissions.READ_WRITE) {
                    return true;
                }
                if (lockMap.size() == 1 && pageLock.getPermissions() == Permissions.READ_ONLY) {
                    pageLock.setPermissions(Permissions.READ_WRITE);
                    lockMap.put(tid, pageLock);
                    pageLockMap.put(pid, lockMap);
                    return true;
                }
            }
        }
        if (pageLock == null) {     // 该事务在该页面上无锁
            if(permissions == Permissions.READ_ONLY) {
                if (lockMap.size() > 1) { // 页面上有多个锁
                    pageLock = new PageLock(tid, Permissions.READ_ONLY);
                    lockMap.put(tid, pageLock);
                    pageLockMap.put(pid, lockMap);
                    return true;
                }
                PageLock lock = null;
                for (PageLock value : lockMap.values()) {   // 此时页面上只有一个锁
                    lock = value;
                }
                if (lock != null && lock.getPermissions() == Permissions.READ_ONLY) {
                    pageLock = new PageLock(tid, Permissions.READ_ONLY);
                    lockMap.put(tid, pageLock);
                    pageLockMap.put(pid, lockMap);
                    return true;
                }
                if (lock != null && lock.getPermissions() == Permissions.READ_WRITE) {
                    wait(50);
                    return false;
                }
            }
            if (permissions == Permissions.READ_WRITE) {
                wait(50);
                return false;
            }
        }
        return true;
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid){
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pid);
        if (lockMap == null) {
            return;
        }
        lockMap.remove(tid);
        if (lockMap.isEmpty()) {
            pageLockMap.remove(pid);
        }
    }
    public synchronized void releaseAllLock(TransactionId tid) {
        for (Map<TransactionId, PageLock> lockMap : pageLockMap.values()) {
            lockMap.remove(tid);
        }
    }
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pid);
        if (lockMap == null) {
            return false;
        }
        PageLock pageLock = lockMap.get(tid);
        if (pageLock == null) {
            return false;
        }
        return true;
    }
}
