package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    private final int numPages;
    private final Map<Integer, Page> pageCache;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lockManager = new LockManager();
        Map<Integer, Page> tmpPageCache = new LinkedHashMap<Integer, Page>(pageSize, 0.75f, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
                boolean res = size() > numPages;
                if (res) {
                    //如果需要淘汰页面，需要判断被淘汰的页面是否为脏页面，如果是脏页面则需要刷入磁盘
                    //否则有数据不一致的风险
                    //实现二阶段严格封锁协议，不能把脏页驱逐出去
                    Page page = eldest.getValue();
                    Integer key = eldest.getKey();
                    if (page.isDirty() != null) {
                        for (Page p : pageCache.values()) {
                            if (p.isDirty() == null) {
                                discardPage(p.getId());
                                return false;
                            }
                        }
                        try {
                            throw new DbException("all pages are dirty page!!!");
                        } catch (DbException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return res;
            }
        };
        Map<Integer, Page> map = Collections.synchronizedMap(tmpPageCache);
        pageCache = map;
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int type;
        if (perm == Permissions.READ_ONLY) {
            type = 0;
        } else {
            type = 1;
        }
        long st = System.currentTimeMillis();
        while (true) {
            //获取锁，如果获取不到会阻塞
            try {
                if (lockManager.requireLock(pid, tid, type)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - st > 500) throw new TransactionAbortedException();
        }
        if(!pageCache.containsKey(pid.hashCode())){
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            pageCache.put(pid.hashCode(), page);
        }
        return pageCache.get(pid.hashCode());
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
    public synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            //如果成功提交，将所有脏页写回瓷盘
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //如果提交失败，回滚，将脏页的原页面写回磁盘
            recoverPages(tid);
        }
        lockManager.completeTransaction(tid);
    }

    private synchronized void recoverPages(TransactionId tid) {
        //从磁盘重新读回page
        for(Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                int tableId = page.getId().getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                Page p = file.readPage(page.getId());
                System.out.println((p.getId() == page.getId()) + "=============");
                pageCache.put(p.getId().hashCode(), p);
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
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        //将页面写到缓存中
        for (Page p : pages) {
            p.markDirty(true, tid);
            pageCache.put(p.getId().hashCode(), p);
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
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = file.deleteTuple(tid, t);
        //将页面写到缓存中
        for (Page p : pages) {
            p.markDirty(true, tid);
            //后期需要保证线程安全
            pageCache.put(p.getId().hashCode(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
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
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        //pageCache实现了LRU, get()方法改变了链表的顺序，pageCache迭代错乱
        //997，999，1001 --> 999,1001,997
        //Page page = pageCache.get(pid.hashCode());
        Page page = null;
        for(Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            Integer key = entry.getKey();
            if (key == pid.hashCode()) {
                page = entry.getValue();
                break;
            }
        }
        DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
        //将脏页保存下来再刷入磁盘
        Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
        Database.getLogFile().force();
        file.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            //要先保存before image
            //更新page的oldData
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
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //使用LinkedHashMap，自带页面淘汰功能
    }

}
