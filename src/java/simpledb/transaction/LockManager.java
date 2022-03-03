package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author hsl
 * @create 2022-03-01 16:29
 */
public class LockManager {
    private ConcurrentMap<PageId, ConcurrentMap<TransactionId, PageLock>> pageLocks;


    public LockManager() {
        pageLocks = new ConcurrentHashMap<>();
    }

    public synchronized boolean requireLock(PageId pid, TransactionId tid, int requireType) throws InterruptedException, TransactionAbortedException {
        final String lockType = requireType == 0 ? "read lock" : "write lock";
        final String thread = Thread.currentThread().getName();

        ConcurrentMap<TransactionId, PageLock> pageLock = pageLocks.get(pid);
        //页面上没有锁
        if (pageLocks.size() == 0 || pageLock == null) {
            PageLock lock = new PageLock(tid, requireType);
            pageLock = new ConcurrentHashMap<>();
            pageLock.put(tid, lock);
            pageLocks.put(pid, pageLock);
            System.out.println(thread + ": the " + pid + " have no lock, transaction" + tid + " require " + lockType + ", accept");
            return true;
        }
        //页面上原本有锁，且该事务对此页面已经加锁
        PageLock lock = pageLock.get(tid);
        if (lock != null) {
            //要求读锁，之前如果是读锁就保持不变，如果是写锁也保持不变（写锁当然有读的权限）
            if (requireType == PageLock.SHARE) {
                System.out.println(thread + ": the " + pid + " have one lock with same txid, transaction" + tid + " require " + lockType + ", accept");
                return true;
            }
            //要求写锁
            if (requireType == PageLock.EXCLUSIVE) {
                //页面锁的数量大于1，全是读锁，写锁被拒绝
                if (pageLock.size() > 1) {
                    System.out.println(thread + ": the " + pid + " have many read locks, transaction" + tid + " require write lock, abort!!!");
                    throw new TransactionAbortedException();
                }
                //该事务之前已加写锁，保持
                if (pageLock.size() == 1 && lock.getType() == PageLock.EXCLUSIVE) {
                    System.out.println(thread + ": the " + pid + " have write lock with same txid, transaction" + tid + " require " + lockType + ", accept");
                    return true;
                }
                //读锁升级为写锁
                if (pageLock.size() == 1 && lock.getType() == PageLock.SHARE) {
                    lock.setType(PageLock.EXCLUSIVE);
                    pageLock.put(tid, lock);
                    pageLocks.put(pid, pageLock);
                    System.out.println(thread + ": the " + pid + " have read lock with same txid, transaction" + tid + " require write lock, accept and upgrade!!!");
                    return true;
                }
            }
        }
        //页面原先有锁，但该事务对此页面没有事先加锁
        if (lock == null) {
            if (requireType == PageLock.SHARE) {
                //有很多读锁，请求新的读锁
                if (pageLock.size() > 1) {
                    PageLock l = new PageLock(tid, requireType);
                    pageLock.put(tid, l);
                    pageLocks.put(pid, pageLock);
                    System.out.println(thread + ": the " + pid + " have many read locks, transaction" + tid + " require " + lockType + ", accept and add a new read lock");
                    return true;
                }
                PageLock one = null;
                for (PageLock l : pageLock.values()) {
                    one = l;
                }
                //原先有一个读锁，请求读锁成功
                if (pageLock.size() == 1 && one.getType() == PageLock.SHARE) {
                    PageLock l = new PageLock(tid, requireType);
                    pageLock.put(tid, l);
                    pageLocks.put(pid, pageLock);
                    System.out.println(thread + ": the " + pid + " have one read lock with diff txid, transaction" + tid + " require read lock, accept and add a new read lock");
                    return true;
                }
                //原先有一个写锁，请求读锁失败
                if (pageLock.size() == 1 && one.getType() == PageLock.EXCLUSIVE) {
                    System.out.println(thread + ": the " + pid + " have one write lock with diff txid, transaction" + tid + " require read lock, await...");
                    wait(50);
                    return false;
                }
            }
            //原先有锁，请求写锁一定失败
            if (requireType == PageLock.EXCLUSIVE) {
                System.out.println(thread + ": the " + pid + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                wait(10);
                return false;
            }
        }
        System.out.println("===========================other case=====================================");
        return true;
    }

    /**
     * 查看指定页面是否被指定事务锁定
     * @param pid
     * @param tid
     * @return
     */
    public synchronized boolean isHoldLock(PageId pid, TransactionId tid) {
        ConcurrentMap<TransactionId, PageLock> map = pageLocks.get(pid);
        if (map == null) return false;
        return map.get(tid) != null;
    }

    /**
     * 释放指定页面的指定事务加的锁
     * @param pid
     * @param tid
     */
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        final String thread = Thread.currentThread().getName();
        ConcurrentMap<TransactionId, PageLock> map = pageLocks.get(pid);
        if (map == null) return;
        if (tid == null) return;
        PageLock lock = map.get(tid);
        if (lock == null) return;
        final String lockType = lock.getType() == 0 ? "read lock" : "write lock";
        map.remove(tid);
        System.out.println(thread + " release " + lockType + " in " + pid + ", the tx lock size is " + map.size() + ", the txid is " + tid);
        //页面上没有锁，删除pagelocks上对应key，否则影响上面的加锁判断
        if (map.size() == 0) {
            pageLocks.remove(pid);
            System.out.println( thread + " release last lock, the page " + pid + " have no lock, the page locks size is " + pageLocks.size() + " the txid is " + tid );
        }
        this.notifyAll();
    }

    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> ids = pageLocks.keySet();
        for (PageId pageId : ids) {
            releaseLock(pageId, tid);
        }
    }
}
