package simpledb;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PageLock
{
    public PageId pageId;
    private Set<TransactionId> sharedLocks;
    private TransactionId exclusiveLock;

    public PageLock(PageId pageId)
    {
        this.pageId = pageId;
        sharedLocks = Collections.synchronizedSet(new HashSet<>());
        exclusiveLock = null;
    }

    public PageId getPageId()
    {
        return pageId;
    }

    public Set<TransactionId> getSharedLocks()
    {
        return sharedLocks;
    }

    public TransactionId getExclusiveLock()
    {
        return exclusiveLock;
    }

    boolean addLock(Permissions perm, TransactionId tid)
    {
        if (perm.equals(Permissions.READ_ONLY))
        {
            if (exclusiveLock != null) return exclusiveLock.equals(tid);
            sharedLocks.add(tid);
            return true;
        } else {
            // exclusive
            if (exclusiveLock != null) return exclusiveLock.equals(tid);
            if (sharedLocks.size() > 1) return false;
            if (sharedLocks.isEmpty() || sharedLocks.contains(tid))
            {
                exclusiveLock = tid;
                sharedLocks.clear();
                return true;
            }
            return false;
        }
    }

    void releaseLock(TransactionId tid)
    {
        assert exclusiveLock == null || tid.equals(exclusiveLock);
        if (tid.equals(exclusiveLock)) exclusiveLock = null;
        else sharedLocks.remove(tid);
    }

    boolean isHolding(TransactionId tid)
    {
        return tid.equals(exclusiveLock) || sharedLocks.contains(tid);
    }

    boolean isExclusive()
    {
        return exclusiveLock != null;
    }

    boolean isExclusiveTid(TransactionId tid)
    {
        if (exclusiveLock == null) return false;
        return exclusiveLock.equals(tid);
    }

    Set<TransactionId> relatedTid()
    {
        Set<TransactionId> tid = new HashSet<>(sharedLocks);
        if (exclusiveLock != null)
        {
            tid.add(exclusiveLock);
        }
        return tid;
    }
}