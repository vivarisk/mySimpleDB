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
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //后面再看看
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();

        RandomAccessFile f = null;
        try{
            f = new RandomAccessFile(file, "r");
            if((pgNo + 1) * pageSize > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[pageSize];
            f.seek(pgNo * pageSize);
            int read = f.read(bytes, 0, pageSize);
            if(read != pageSize){
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, bytes);

        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                f.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        if(pgNo > numPages()){
            throw new IllegalArgumentException();
        }
        int pgSize = BufferPool.getPageSize();
        //write IO
        RandomAccessFile f = new RandomAccessFile(file,"rw");
        // set offset
        f.seek(pgNo*pgSize);
        // write
        byte[] data = page.getPageData();
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //以后再看看：如果文件最后不够一页呢？
        int num = (int) Math.floor((file.length() * 1.0) / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageList= new ArrayList<>();
        for(int i=0;i<numPages();++i){
            // took care of getting new page
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid,Permissions.READ_WRITE);
            //虽然违背严格两阶段协议，看lab4 exercise2的说明
            if(p.getNumEmptySlots() == 0){
                Database.getBufferPool().unsafeReleasePage(tid, pid);
                continue;
            }
            p.insertTuple(t);
            p.markDirty(true, tid);
            pageList.add(p);
            return pageList;
        }
        // no new page
        //写一块空的进去（一张页面的大小）numPages()已经变了
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        // load into cache
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
        p.insertTuple(t);
        p.markDirty(true, tid);
        pageList.add(p);
        return pageList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageList = new ArrayList<Page>();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                t.getRecordId().getPageId(),Permissions.READ_WRITE);
        p.deleteTuple(t);
        p.markDirty(true, tid);
        pageList.add(p);
        return pageList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    //很懵，以后再看看
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    /**
     * HeapFile迭代器，用于遍历HeapFile的所有tuple；
     * 需要使用上BufferPool.getPage(),注意一次不能读出HeapFile的所有tuples，不然会出现OOM
     */
    private static class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private final HeapFile file;
        private Iterator<Tuple> it;
        private int pageNo;
        private Tuple next;
        private boolean open;

        public HeapFileIterator(TransactionId tid, HeapFile file) {
            this.tid = tid;
            this.file = file;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            this.pageNo = 0;
            it = getTupleIterator(pageNo);
        }

        /**
         * 根据pageNo从buffer pool 或者磁盘读出HeapPage并返回其tuple的迭代器
         * @param pageNo
         * @return
         * @throws TransactionAbortedException
         * @throws DbException
         */
        private Iterator<Tuple> getTupleIterator(int pageNo) throws TransactionAbortedException, DbException{
            if(pageNo >= 0 && pageNo < file.numPages()) {
                HeapPageId pid = new HeapPageId(file.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                if(page == null) throw new DbException("get iterator fail! pageNo #" + pageNo + "# is invalid!");
                Iterator<Tuple> it = page.iterator();
                return it;
            }
            throw new DbException("get iterator fail!!! pageNo #" + pageNo + "# is invalid!");
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            //需要先判断文件有没有打开
            if (!this.open) return false;
            if (next == null) next = fetchNext();
            return next != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.open) throw new NoSuchElementException("Not open!");
            if (next == null) {
                next = fetchNext();
                if (next == null) {
                    throw new NoSuchElementException("iterator don't have next");
                }
            }
            Tuple result = next;
            next = null;
            return result;
        }

        private Tuple fetchNext() throws TransactionAbortedException, DbException {
            if(!it.hasNext()) {
                //往后面寻找下一个不为空的数据页
                while(pageNo < file.numPages() - 1) {
                    pageNo ++;
                    it = getTupleIterator(pageNo);
                    if (it.hasNext()) return it.next();
                }
                //如果后面的数据页都为空，则返回null
                return null;
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
            open = false;
        }
    }


}

