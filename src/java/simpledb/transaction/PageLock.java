package simpledb.transaction;

/**
 * @author hsl
 * @create 2022-03-01 16:27
 */
public class PageLock {
    public static final int SHARE = 0;
    public static final int EXCLUSIVE = 1;
    private TransactionId tid;
    private int type;
    public PageLock(TransactionId tid, int type) {
        this.tid = tid;
        this.type = type;
    }

    public int getType() {
        return this.type;
    }

    public TransactionId getTid() {
        return this.tid;
    }

    public void setType(int type) {
        this.type = type;
    }
}
