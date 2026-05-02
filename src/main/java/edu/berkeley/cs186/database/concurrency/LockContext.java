package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        // ResourceName 之所以要设计成: ["Database", "Table1", "Row10"],
        // 因为这里可以迭代器找到那一串的数据结构的叶节点, 也就是 "Row10"
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // 1. 校验当前资源只读
        if(this.readonly) throw new UnsupportedOperationException("Read only locks are not supported");

        // 2. 校验当前资源锁类型
        if (lockType == LockType.NL) throw new InvalidLockException("Cannot acquire NL lock, use release instead");

        // 3. 校验当前资源申请重复锁
        long transNum = transaction.getTransNum();
        if(!lockman.getLockType(transaction,name).equals(LockType.NL)) throw new DuplicateLockRequestException("Transaction " + transNum + " already holds a lock on " + name);

        // 4. 校验父级锁与当前申请锁是否兼容
        if (parent != null) {
            LockType parentLockType = lockman.getLockType(transaction, parent.getResourceName());
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("Parent lock " + parentLockType + " does not allow child lock " + lockType);
            }
        }

        // 实际LockManager处理锁
        lockman.acquire(transaction, name, lockType);

        // 后置处理: 增加当前资源节点的父级增加childLocks
        if (parent != null) parent.numChildLocks.put(transNum, parent.numChildLocks.getOrDefault(transNum, 0) + 1);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // 1. 检查只读模式下不支持锁释放
        if (this.readonly) throw new UnsupportedOperationException("Read only locks are not supported");

        long transNum = transaction.getTransNum();
        LockType currentLock = lockman.getLockType(transaction, name);

        // 2. 检查事务是否持有锁
        if (currentLock == LockType.NL) throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);

        // 3. 锁释放检查: 必须先释放所有子锁才能释放父锁
        // 禁止在子锁存在时释放，保证锁粒度安全
        int childLockCount = numChildLocks.getOrDefault(transNum, 0);
        if (childLockCount > 0) throw new InvalidLockException("Cannot release lock when child locks are held");

        // 4. 实际LockManager执行锁释放
        lockman.release(transaction, name);

        // 5. 后置处理: 父节点子锁计数 - 1
        if (parent != null) {
            int parentChildCount = parent.numChildLocks.getOrDefault(transNum, 0);
            if (parentChildCount > 0) {
                parent.numChildLocks.put(transNum, parentChildCount - 1);
            }
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // 1. 检查只读模式
        if(this.readonly) throw new UnsupportedOperationException("Read only locks are not supported");

        long transNum = transaction.getTransNum();
        LockType currentLockType = lockman.getLockType(transaction, name);
        // 2. 检查无锁
        if(currentLockType.equals(LockType.NL)) throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);

        // 3. 检查重复锁申请
        if(currentLockType.equals(newLockType))throw new DuplicateLockRequestException("Transaction " + transNum + " already holds " + newLockType + " lock");

        // 4. 检查申请锁与当前资源持有锁的权限大小
        if (!LockType.substitutable(newLockType, currentLockType)) throw new InvalidLockException("Cannot promote " + currentLockType + " to " + newLockType);

        // 5. 检查当前申请锁为SIX且存在祖先已持有SIX
        if (newLockType == LockType.SIX && hasSIXAncestor(transaction)) throw new InvalidLockException("Cannot promote to SIX with SIX ancestor");

        // 6. 如果升级到 SIX，需要释放所有 S/IS 后代锁
        if (newLockType == LockType.SIX
                && (currentLockType == LockType.IS || currentLockType == LockType.IX)) {
            // 获取当前资源为父级，所有的子孙级待释放的IS/S的资源
            List<ResourceName> descendants = sisDescendants(transaction);
            List<ResourceName> locksToRelease = new ArrayList<>(descendants);
            // 添加当前资源的待释放资源
            locksToRelease.add(name);
            // 实际处理锁释放
            lockman.acquireAndRelease(transaction, name, newLockType, locksToRelease);
            // 后置处理
            int releasedCount = descendants.size();
            int currentCount = numChildLocks.getOrDefault(transNum, 0);
            numChildLocks.put(transNum, Math.max(0, currentCount - releasedCount));
        } else {
            // 普通升级
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // 1. 检查readonlu
        if (readonly) throw new UnsupportedOperationException("Read only locks are not supported");

        long transNum = transaction.getTransNum();
        LockType currentLockType = lockman.getLockType(transaction, name);
        // 2. 检查无锁
        if (currentLockType.equals(LockType.NL)) throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);

        // 3. 获取所有子孙锁
        List<ResourceName> descendantLocks = getAllDescendantLocks(transaction);

        LockType newLockType;
        // 4. 处理升级逻辑
        if (descendantLocks.isEmpty()) {
            // 如果没有后代锁，根据当前锁类型决定升级
            if (currentLockType == LockType.IS) {
                newLockType = LockType.S;
            } else if (currentLockType == LockType.IX) {
                newLockType = LockType.X;
            } else {
                return; // 如果当前是 S/X/SIX 且没有后代锁，不需要升级
            }
        } else {
            // 如果有后代锁，检查是否需要 X 锁
            newLockType = hasXInDecendantLocks(transaction, descendantLocks) ? LockType.X : LockType.S;
        }

        // 5. 如果新旧锁类型相同，说明没有实际的升级，直接返回
        if (currentLockType.equals(newLockType)) return;

        // 6. 准备要释放的锁列表: 包括当前资源上的旧锁和所有后代锁
        List<ResourceName> locksToRelease = new ArrayList<>(descendantLocks);
        locksToRelease.add(name); // 添加本身

        // 7. 实际执行
        lockman.acquireAndRelease(transaction, name, newLockType, locksToRelease);

        // 8. 清理所有后代锁在父节点中的计数
        for (ResourceName descName : descendantLocks) {
            LockContext childContext = LockContext.fromResourceName(lockman, descName);
            // NOTE: 假设 descName: page1; 当前 name 是database
            // 结构: ["database", "table1", "page3", "row10"]
            // LockContext.fromResourceName(lockman, descName) 返回的是: lockContext(page1)
            LockContext parentContext = childContext.parentContext();
            // 那么 parentContext: table1
            // 向上遍历父节点，直到当前升级节点，统一减去子锁计数
            while (parentContext != null && !parentContext.name.equals(name)) {
                // NOTE: 从row10 向上遍历直到database 也就是name这个变量
                int count = parentContext.numChildLocks.getOrDefault(transNum, 0);
                if (count > 0) parentContext.numChildLocks.put(transNum, count - 1);
                // 向上递归, 直至database, 即当前升级的节点
                parentContext = parentContext.parentContext();
            }
        }

        // 所有后代锁已释放，当前节点子锁计数归零
        numChildLocks.put(transNum, 0);

    }

    private boolean hasXInDecendantLocks(TransactionContext transaction, List<ResourceName> descendantLocks) {
        for(ResourceName childLock : descendantLocks){
            LockType childLockType = lockman.getLockType(transaction, childLock);
            // 只要有一个后代是 X, IX, SIX (意味着有写入意图或权限), 就需要升级到 X
            if(childLockType.equals(LockType.X) || childLockType.equals(LockType.IX) || childLockType.equals(LockType.SIX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取所有子孙锁
     * @param transaction
     * @return
     */
    private List<ResourceName> getAllDescendantLocks(TransactionContext transaction) {
        List<ResourceName> result = new ArrayList<>();
        for (Map.Entry<String, LockContext> entry : children.entrySet()) {
            LockContext child = entry.getValue();
            LockType lockType = child.lockman.getLockType(transaction, child.getResourceName());
            if (!lockType.equals(LockType.NL)) {
                result.add(child.getResourceName());
            }

            result.addAll(child.getAllDescendantLocks(transaction));
        }
        return result;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        LockType explicitLock = lockman.getLockType(transaction, name);
        if (explicitLock != LockType.NL) return explicitLock;

        LockContext current = this.parent;
        while (current != null) {
            LockType parentLockType = current.lockman.getLockType(transaction, current.getResourceName());
            if (parentLockType == LockType.S || parentLockType == LockType.X || parentLockType == LockType.SIX) {
                return LockType.S; // S, X, SIX 都隐式授予了 S 权限
            }
            current = current.parent;
        }
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext current = this.parent;
        while (current != null) {
            LockType lockType = current.lockman.getLockType(transaction, current.getResourceName());
            if (lockType == LockType.SIX) return true;
            current = current.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<ResourceName> result = new ArrayList<>();
        for (Map.Entry<String, LockContext> entry : children.entrySet()) {
            LockContext child = entry.getValue();
            LockType lockType = child.lockman.getLockType(transaction, child.getResourceName());
            if (lockType == LockType.S || lockType == LockType.IS) result.add(child.getResourceName());

            result.addAll(child.sisDescendants(transaction));
        }
        return result;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

