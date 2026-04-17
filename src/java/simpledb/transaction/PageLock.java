package simpledb.transaction;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.execution.Predicate;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class PageLock {
    private TransactionId transactionId;
    private Permissions permissions;

    public PageLock(TransactionId transactionId, Permissions permissions) {
        this.transactionId = transactionId;
        this.permissions = permissions;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public Permissions getPermissions() {
        return permissions;
    }

}
