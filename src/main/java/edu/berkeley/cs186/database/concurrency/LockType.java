package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (b == S) return a == S || a == IS || a == NL;
        if (b == X) return a == NL;
        if (b == IS) return a == S || a == IS || a == IX || a == SIX || a == NL;
        if (b == IX) return a == IS || a == IX || a == NL;
        if (b == SIX) return a == IS || a == NL;
        if (b == NL) return true;

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }

        // 父锁必须满足意向权限 >= 子锁需要的意向锁
        if (parentLockType == NL || parentLockType == S) return childLockType == NL;
        if (parentLockType == IS) return childLockType == IS || childLockType == S || childLockType == NL;
        if (parentLockType == IX || parentLockType == SIX || parentLockType == X) return true;
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // 替代规则：权限 >= 需要的权限
        if (substitute == NL) return required == NL;
        if (substitute == IS) return required == IS || required == NL;
        if (substitute == IX) return required == IX || required == IS || required == NL;
        if (substitute == S) return required != X && required != IX && required != SIX && required != IS;
        if (substitute == SIX) return required == NL || required == IS || required == IX || required == S || required == SIX;
        if (substitute == X) return true;

        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

