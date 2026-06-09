package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    // 存储所有<事物id,list(事物里所有锁)>
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    // 存储所有<资源名称, 对应资源>
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        // 每个资源中授权的锁
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        // 存储每个资源等待锁对应的请求参与排队,
        // LockRequest 实际上存的是 事物 + 锁 + releasedLocks 特殊性: 处理的是当事物加上锁时, 需要释放的冗余锁
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for (Lock lock : locks) {
                long tid = lock.transactionNum;
                // 跳过自己的事物
                if (tid == except) continue;

                // 只要有一个其他事物中的锁不兼容
                if (!LockType.compatible(lock.lockType, lockType)) return false;
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            long transNum = lock.transactionNum;
            // 1. 查找当前事务已持有的锁
            Lock existingLock = locks.stream()
                    .filter(l -> l.transactionNum == transNum)
                    .findFirst()
                    .orElse(null);

            // 2. 处理资源锁
            if (existingLock != null) locks.remove(existingLock);
            locks.add(lock);

            // 3. 处理事务锁
            List<Lock> txLockList = transactionLocks.computeIfAbsent(transNum, k -> new ArrayList<>());
            if (existingLock != null) txLockList.remove(existingLock);
            txLockList.add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            locks.remove(lock);

            long transNum = lock.transactionNum;
            List<Lock> heldLocks = transactionLocks.get(transNum);
            if (heldLocks != null) {
                heldLocks.remove(lock);
                if (heldLocks.isEmpty()) {
                    transactionLocks.remove(transNum);
                }
            }

            // 释放锁后, 尝试唤醒等待队列
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if(addFront) waitingQueue.addFirst(request);
            else waitingQueue.addLast(request);
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            // 1. 获取当前waitingQueue
            Iterator<LockRequest> iterator = waitingQueue.iterator();

            while (iterator.hasNext()) {
                LockRequest request = iterator.next();
                Lock lock = request.lock;

                // 2. 检查锁是否兼容
                if (checkCompatible(lock.lockType, lock.transactionNum)) {
                    // 兼容
                    grantOrUpdateLock(lock);    // 授予锁
                    iterator.remove();  // 移除请求
                    request.transaction.unblock();  // 唤醒事务
                } else {
                    // 不兼容, 停止处理后续请求
                    break;
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for(Lock lock : locks){
                if(lock.transactionNum == transaction){
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     * 原子性的为当前事物获取的锁且释放releaseNames冗余的锁
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     * 错误检查发生在任何锁的获取或者释放，如果新锁不兼容，那么事物堵塞住放入队列待下次唤醒
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     * 在获取锁之后，releaseNames释放，接着队列需要处理
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     * 升级锁不能改变时间
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * 如果一个锁已经被当前事物获取且没有释放，那么直接扔出异常，
     * 同一个事务，不允许对同一个资源发起「重复锁请求」哪怕锁类型一样、兼容，也禁止重复申请
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     * 如果事务没有持有 releaseNames 列表中任意一个 / 多个 资源的锁，就抛出 NoLockHeldException
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {

        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);

            // 1. 判断重复锁
            LockType currentLock = entry.getTransactionLockType(transNum);
            if (currentLock == lockType && !releaseNames.contains(name)) {
                throw new DuplicateLockRequestException("Transaction " + transNum + " already holds a lock on " + name);
            }

            // 2. 检查释放的锁是否都持有
            for (ResourceName releaseName : releaseNames) {
                // 获取每个待释放的锁
                ResourceEntry releaseEntry = getResourceEntry(releaseName);
                if (releaseEntry.getTransactionLockType(transNum) == LockType.NL) {
                    throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + releaseName);
                }
            }

            // 3. 检查兼容性
            if (!entry.checkCompatible(lockType, transNum)) {
                shouldBlock = true;
                Lock newLock = new Lock(name, lockType, transNum);
                LockRequest request = new LockRequest(transaction, newLock);
                entry.addToQueue(request, true);
            } else {
                // 获取新锁
                Lock newLock = new Lock(name, lockType, transNum);
                entry.grantOrUpdateLock(newLock);
                // 遍历待释放的资源
                for (ResourceName releaseName : releaseNames) {
                    if (!releaseName.equals(name)) {  // 跳过当前资源
                        ResourceEntry releaseEntry = getResourceEntry(releaseName);
                        Lock lockToRelease = null;
                        // 寻找待释放资源的中自己的锁
                        for (Lock lock : releaseEntry.locks) {
                            if (lock.transactionNum == transNum) {
                                lockToRelease = lock;
                                break;
                            }
                        }
                        if (lockToRelease != null) {
                            releaseEntry.releaseLock(lockToRelease);
                        }
                    }
                }
            }
        }

        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }


    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name, LockType lockType)
            throws DuplicateLockRequestException {

        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);

            // 1. 检查事务已持有该资源锁, 重复加锁异常
            LockType currentLock = entry.getTransactionLockType(transNum);
            if (currentLock != LockType.NL) throw new DuplicateLockRequestException("Transaction " + transNum + " already holds " + currentLock + " on " + name);

            // 2. 判断是否需要排队,不兼容 or 已有等待队列
            boolean notCompatible = !entry.checkCompatible(lockType, transNum);
            boolean hasWaiting = !entry.waitingQueue.isEmpty();

            if (notCompatible || hasWaiting) {
                shouldBlock = true;
                Lock newLock = new Lock(name, lockType, transNum);
                // 普通排队, 队尾添加
                entry.addToQueue(new LockRequest(transaction, newLock), false);
            } else {    // 兼容且队列无锁等待
                // 3. 直接加锁
                Lock newLock = new Lock(name, lockType, transNum);
                entry.grantOrUpdateLock(newLock);
            }
        }

        // 4. 同步块外阻塞
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {

        long transNum = transaction.getTransNum();
        ResourceEntry entry = getResourceEntry(name);

        synchronized (this) {
            // 校验事务未持有锁
            if (entry.getTransactionLockType(transNum) == LockType.NL)
                throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);

            // 查找当前事务在该资源上持有的锁
            for (Lock lock : entry.locks) {
                if (lock.transactionNum == transNum) {
                    entry.releaseLock(lock);
                    break;
                }
            }
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            LockType currentLock = entry.getTransactionLockType(transNum);
            // 1. 前置校验
            if (currentLock == LockType.NL)
                throw new NoLockHeldException("Transaction " + transNum + " does not hold a lock on " + name);
            if (currentLock == newLockType)
                throw new DuplicateLockRequestException("Transaction " + transNum + " already has a lock of type " + newLockType + " on " + name);
            if (!LockType.substitutable(newLockType, currentLock))
                throw new InvalidLockException("Cannot promote from " + currentLock + " to " + newLockType + " - not substitutable");

            // 2. 校验是否兼容
            if (!entry.checkCompatible(newLockType, transNum)) {
                shouldBlock = true;
                Lock newLock = new Lock(name, newLockType, transNum);
                LockRequest request = new LockRequest(transaction, newLock);
                entry.addToQueue(request, true);
            } else {
                Lock newLock = new Lock(name, newLockType, transNum);
                entry.grantOrUpdateLock(newLock);
            }
        }

        // 3. 同步块外阻塞
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }

    /**
     * Dump all lock information for debugging deadlock scenarios.
     * Shows all transaction locks and resource entries.
     */
    public synchronized String getAllLockInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== LockManager State ===\n");
        sb.append("transactionLocks: ").append(transactionLocks).append("\n");
        sb.append("resourceEntries:\n");
        for (Map.Entry<ResourceName, ResourceEntry> entry : resourceEntries.entrySet()) {
            sb.append("  ").append(entry.getKey()).append(" => ")
              .append(entry.getValue().toString()).append("\n");
        }
        return sb.toString();
    }
}
