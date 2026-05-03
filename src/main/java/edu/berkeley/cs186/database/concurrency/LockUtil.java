package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        // 获取有效锁 (如果当前资源有非 NL，那么就是它，否则从父级别寻找)
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        // 获取显示锁 (即本身资源上的锁类型)
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // Case 1: 如果请求为 NL && 只要存在显示锁不是 NL -> 释放
        if (requestType == LockType.NL && !explicitLockType.equals(LockType.NL)) {
            lockContext.release(transaction); return;
        }

        // Case 2：当前有效锁类型已经能替代请求的锁类型
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // Case 3：当前是 IX 锁 + 请求 S 锁，升级为 SIX
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX); return;
        }

        // Case 4：当前是 IS 锁 + 请求是 S 锁，需要升级至 S
        if (explicitLockType == LockType.IS && requestType == LockType.S) {
            lockContext.escalate(transaction); return;
        }

        // Case 5：当前是 IX 锁 + 请求是 X 锁，需要升级至 X
        if (explicitLockType == LockType.IX && requestType == LockType.X) {
            lockContext.escalate(transaction); return;
        }

        // Case 6：当前是 S 锁 + 请求是 X 锁，升级至 X
        if (explicitLockType == LockType.S && requestType == LockType.X) {
            // 确保祖先有足够的意向锁，即祖先已经持有IX
            ensureAncestorIntentLocks(lockContext, requestType);
            lockContext.promote(transaction, LockType.X);
            return;
        }

        // 情况4：当前没有锁，需要从头获取
        if (explicitLockType == LockType.NL) {
            // 确保祖先有足够的意向锁
            ensureAncestorIntentLocks(lockContext, requestType);
            lockContext.acquire(transaction, requestType);
            return;
        }
    }

    /**
     * 当前节点上的所有祖先优先获得足够强的意向锁
     * @param lockContext
     * @param requestType
     */
    private static void ensureAncestorIntentLocks(LockContext lockContext, LockType requestType) {
        LockContext parentContext = lockContext.parentContext();
        if (parentContext == null) return;

        TransactionContext transaction = TransactionContext.getTransaction();

        // 1. 获取足够权限的意向锁
        LockType neededIntentLock = getNeededIntentLock(requestType);

        // 2. 祖先有合适的锁
        ensureAncestorIntentLocks(parentContext, neededIntentLock);

        LockType parentExplicitLock = parentContext.getExplicitLockType(transaction);
        LockType parentEffectiveLock = parentContext.getEffectiveLockType(transaction);

        // 3. 如果父节点的有效锁已经能满足需求，不需要做任何事
        if (LockType.substitutable(parentEffectiveLock, neededIntentLock)) return;

        // 4. 如果父节点没有显式锁，获取意向锁
        if (parentExplicitLock == LockType.NL) {
            parentContext.acquire(transaction, neededIntentLock);
        }
        // 如果父节点的显式锁不够强，需要升级
        else if (!LockType.substitutable(parentExplicitLock, neededIntentLock)) {

            if (parentExplicitLock == LockType.IS && neededIntentLock == LockType.IX) {
                parentContext.promote(transaction, LockType.IX);
            }

            else if (parentExplicitLock == LockType.S && neededIntentLock == LockType.IX) {
                parentContext.promote(transaction, LockType.SIX);
            }
        }
    }

    /*
     * 根据需要上锁的锁类型判断父级别需要获取的意向锁
     */
    private static LockType getNeededIntentLock(LockType requestType) {
        // 1. 父节点需要的意向锁类型
        LockType neededIntentLock;
        if (requestType == LockType.S || requestType == LockType.IS) {
            neededIntentLock = LockType.IS;
        } else {
            // requestType == LockType.X || requestType == LockType.IX
            neededIntentLock = LockType.IX;
        }
        return neededIntentLock;
    }

}
