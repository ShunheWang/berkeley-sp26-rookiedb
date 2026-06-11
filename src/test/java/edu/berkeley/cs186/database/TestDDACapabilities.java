package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.table.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Tests for rookieDB DDA capabilities added for cross-connection deadlock handling.
 *
 * Tests cover:
 *   - Transaction registry (register/remove lifecycle)
 *   - Transaction start time recording
 *   - getAllLockInfo output with transactionTimes
 *   - Cross-thread unsetTransaction(long transNum)
 *   - LockManager.removeFromAllQueues()
 *   - Database.rollbackTransaction() — basic, blocked, not-found
 *
 * These tests use a real Database with ARIESRecoveryManager (recovery=true),
 * matching the production Server configuration.
 */
public class TestDDACapabilities {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String testDir;
    private Database db;
    private Schema testSchema;

    @Before
    public void setUp() throws IOException {
        testDir = tempFolder.newFolder("test-dir").getAbsolutePath();
        testSchema = new Schema()
                .add("id", Type.intType())
                .add("value", Type.stringType(20));
    }

    /**
     * Create a fresh Database with ARIES recovery and a test table.
     */
    private Database createDatabase() {
        Database database = new Database(testDir, 64, new LockManager(),
                new ClockEvictionPolicy(), true);
        try (Transaction t = database.beginTransaction()) {
            t.createTable(testSchema, "items");
        }
        return database;
    }

    @After
    public void tearDown() {
        if (db != null) {
            db.close();
        }
        db = null;
    }

    // =========================================================================
    // Transaction Registry Tests
    // =========================================================================

    /**
     * Transaction is registered in beginTransaction() and removed after commit.
     */
    @Test
    public void testRegistryRegisterAndCommit() {
        db = createDatabase();
        long transNum;
        try (Transaction t = db.beginTransaction()) {
            transNum = t.getTransNum();
            assertNotNull("transaction should be in registry after begin",
                    db.transactionRegistry.get(transNum));
            assertTrue("transaction should appear in getTransactionTimes",
                    db.getTransactionTimes().containsKey(transNum));
        }
        // After try-with-resources (auto-commit via close()), registry is clean
        assertNull("transaction should be removed from registry after commit",
                db.transactionRegistry.get(transNum));
        assertFalse("transaction should not appear in getTransactionTimes",
                db.getTransactionTimes().containsKey(transNum));
    }

    /**
     * Transaction is registered in beginTransaction() and removed after rollback.
     */
    @Test
    public void testRegistryRegisterAndRollback() {
        db = createDatabase();
        long transNum;
        Transaction t = db.beginTransaction();
        transNum = t.getTransNum();
        assertNotNull("transaction should be in registry after begin",
                db.transactionRegistry.get(transNum));

        t.rollback();
        t.close();

        assertNull("transaction should be removed from registry after rollback",
                db.transactionRegistry.get(transNum));
    }

    /**
     * Multiple transactions are correctly registered and removed independently.
     */
    @Test
    public void testRegistryMultipleTransactions() {
        db = createDatabase();
        Transaction t1 = db.beginTransaction();
        Transaction t2 = db.beginTransaction();
        Transaction t3 = db.beginTransaction();

        assertEquals("all 3 transactions should be in registry",
                3, db.transactionRegistry.size());
        assertEquals("all 3 should have start times",
                3, db.getTransactionTimes().size());

        t2.commit();
        t2.close();

        assertEquals("registry should shrink to 2 after t2 commits",
                2, db.transactionRegistry.size());
        assertNull("t2 should be removed", db.transactionRegistry.get(t2.getTransNum()));
        assertNotNull("t1 still there", db.transactionRegistry.get(t1.getTransNum()));
        assertNotNull("t3 still there", db.transactionRegistry.get(t3.getTransNum()));

        t1.rollback(); t1.close();
        t3.commit(); t3.close();

        assertEquals("registry should be empty after all done",
                0, db.transactionRegistry.size());
    }

    // =========================================================================
    // Transaction Start Time Tests
    // =========================================================================

    @Test
    public void testStartTimeRecorded() {
        db = createDatabase();
        long before = System.currentTimeMillis();
        try (Transaction t = db.beginTransaction()) {
            long startTime = db.getTransactionTimes().get(t.getTransNum());
            assertTrue("startTime should be >= wall clock before begin",
                    startTime >= before);
            assertTrue("startTime should be <= wall clock now",
                    startTime <= System.currentTimeMillis());
        }
    }

    @Test
    public void testYoungestTransaction() throws InterruptedException {
        db = createDatabase();
        Transaction t1 = db.beginTransaction();
        Thread.sleep(5); // ensure measurable time gap
        Transaction t2 = db.beginTransaction();

        long time1 = db.getTransactionTimes().get(t1.getTransNum());
        long time2 = db.getTransactionTimes().get(t2.getTransNum());

        assertTrue("t2 should have later or equal startTime than t1",
                time2 >= time1);

        t1.commit(); t1.close();
        t2.commit(); t2.close();
    }

    @Test
    public void testGetTransactionTimesEmpty() {
        db = createDatabase();
        // No active transactions → empty map
        Map<Long, Long> times = db.getTransactionTimes();
        assertTrue("should be empty with no active transactions", times.isEmpty());
    }

    // =========================================================================
    // getAllLockInfo Tests
    // =========================================================================

    @Test
    public void testGetAllLockInfoIncludesTransactionTimes() {
        db = createDatabase();
        try (Transaction t = db.beginTransaction()) {
            String info = db.getAllLockInfo();

            // Should contain the LockManager output
            assertTrue("should contain LockManager header",
                    info.contains("=== LockManager State ==="));
            assertTrue("should contain transactionLocks",
                    info.contains("transactionLocks:"));

            // Should contain the appended transactionTimes
            assertTrue("should contain transactionTimes section",
                    info.contains("transactionTimes: {"));
            assertTrue("should contain transaction number",
                    info.contains(String.valueOf(t.getTransNum())));
        }
    }

    @Test
    public void testGetAllLockInfoEmptyTransactions() {
        db = createDatabase();
        String info = db.getAllLockInfo();

        assertTrue("should contain LockManager state",
                info.contains("=== LockManager State ==="));
        assertTrue("should contain transactionTimes even when empty",
                info.contains("transactionTimes: {"));
    }

    // =========================================================================
    // Cross-Thread unsetTransaction(long) Tests
    // =========================================================================

    /**
     * unsetTransaction(long transNum) should work from any thread.
     * We verify by calling it from a different thread than the one
     * that owns the transaction.
     */
    @Test
    public void testUnsetTransactionFromDifferentThread() throws Exception {
        db = createDatabase();
        CountDownLatch tReady = new CountDownLatch(1);
        CountDownLatch unsetDone = new CountDownLatch(1);
        AtomicLong transNum = new AtomicLong();

        // Start a transaction on a background thread
        Thread tThread = new Thread(() -> {
            Transaction t = db.beginTransaction();
            transNum.set(t.getTransNum());
            assertNotNull("thread-local context should be set on owner thread",
                    TransactionContext.getTransaction());
            tReady.countDown();

            try {
                unsetDone.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // After unset, the thread-local should be gone
            assertNull("thread-local should be null after external unset",
                    TransactionContext.getTransaction());
        });
        tThread.start();

        assertTrue("background thread should be ready", tReady.await(5, TimeUnit.SECONDS));
        Thread.sleep(100); // let transaction settle

        // Call unset from the MAIN thread (different thread!)
        long num = transNum.get();
        TransactionContext.unsetTransaction(num);
        unsetDone.countDown();

        tThread.join(5000);

        // Registry should not have the transaction anymore (unset ≠ remove from registry)
        // Actually, unsetTransaction only removes from threadTransactions map.
        // The transactionRegistry is separate. The transaction is still in registry
        // until cleanup() runs. Let's verify that distinction.
        assertNotNull("transaction should still be in registry after unset",
                db.transactionRegistry.get(num));
    }

    // =========================================================================
    // removeFromAllQueues Tests
    // =========================================================================

    /**
     * When a transaction is waiting in a resource queue, removeFromAllQueues
     * should clear its pending request.
     */
    @Test
    public void testRemoveFromAllQueuesClearsPendingRequest() throws Exception {
        db = createDatabase();
        LockManager lockManager = db.getLockManager();
        ResourceName resource = new ResourceName("test_resource");

        // T1: hold X lock on the resource
        Transaction t1 = db.beginTransaction();
        lockManager.acquire(t1.getTransactionContext(), resource, LockType.X);

        // T2: try to acquire X → will block (on a separate thread)
        CountDownLatch t2Blocked = new CountDownLatch(1);
        CountDownLatch t2Resolved = new CountDownLatch(1);
        AtomicLong t2num = new AtomicLong();
        AtomicReference<String> t2Result = new AtomicReference<>("unknown");

        Thread t2Thread = new Thread(() -> {
            try {
                Transaction t2 = db.beginTransaction();
                t2num.set(t2.getTransNum());

                // Insert a row to acquire metadata locks (ensures transaction appears in alllocks)
                t2.insert("items", new edu.berkeley.cs186.database.table.Record(999, "test"));

                t2Blocked.countDown();
                Thread.sleep(200); // let main thread read state

                // Now try to acquire X on the same resource → will block
                lockManager.acquire(t2.getTransactionContext(), resource, LockType.X);

                t2Result.set("got lock unexpectedly");
                t2.rollback();
                t2.close();
            } catch (Exception e) {
                t2Result.set("exception: " + e.getMessage());
            } finally {
                t2Resolved.countDown();
            }
        });
        t2Thread.start();

        assertTrue("T2 should be ready", t2Blocked.await(5, TimeUnit.SECONDS));
        Thread.sleep(300); // let T2 enter block() state

        // Verify T2 is in the waiting queue
        String lockInfo = lockManager.getAllLockInfo();
        assertTrue("T2 should appear in alllocks", lockInfo.contains("T" + t2num.get()));

        // Now: remove T2 from all queues
        lockManager.removeFromAllQueues(t2num.get());

        // After queue removal, find and unblock the victim thread
        TransactionContext victimCtx = null;
        for (TransactionContext ctx : TransactionContext.threadTransactions.values()) {
            if (ctx.getTransNum() == t2num.get()) {
                victimCtx = ctx;
                break;
            }
        }
        assertNotNull("victim context should be found", victimCtx);
        victimCtx.unblock();

        assertTrue("T2 should finish within 5s", t2Resolved.await(5, TimeUnit.SECONDS));
        assertTrue("T2 should not have gotten the lock",
                t2Result.get().contains("exception"));

        // Clean up T1
        t1.rollback();
        t1.close();
        t2Thread.join(5000);
    }

    // =========================================================================
    // rollbackTransaction() Tests
    // =========================================================================

    /**
     * Kill a non-existent transaction → IllegalArgumentException.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRollbackTransactionNotFound() {
        db = createDatabase();
        db.rollbackTransaction(99999L);
    }

    /**
     * Kill a RUNNING transaction (not blocked, no locks held).
     */
    @Test
    public void testRollbackTransactionRunning() {
        db = createDatabase();
        long transNum;
        Transaction t = db.beginTransaction();
        transNum = t.getTransNum();

        // Kill it from "another connection" (same thread, simulating DDA)
        db.rollbackTransaction(transNum);

        // Registry should be clean
        assertNull("transaction should be removed from registry",
                db.transactionRegistry.get(transNum));

        // The victim Transaction object is now dead; close is a no-op
        t.close();
    }

    /**
     * Kill a transaction blocked on a lock held by another transaction.
     * This is the core DDA deadlock victim scenario.
     */
    @Test
    public void testRollbackTransactionBlocked() throws Exception {
        db = createDatabase();
        LockManager lockManager = db.getLockManager();
        ResourceName resource = new ResourceName("test_resource");

        // T1: hold X lock
        Transaction t1 = db.beginTransaction();
        lockManager.acquire(t1.getTransactionContext(), resource, LockType.X);

        // T2: block on X lock in separate thread
        CountDownLatch t2Blocked = new CountDownLatch(1);
        CountDownLatch t2Finished = new CountDownLatch(1);
        AtomicLong t2num = new AtomicLong();
        AtomicReference<String> t2Result = new AtomicReference<>("unknown");

        Thread t2Thread = new Thread(() -> {
            try {
                Transaction t2 = db.beginTransaction();
                t2num.set(t2.getTransNum());
                t2Blocked.countDown();
                Thread.sleep(200);

                // This will block
                lockManager.acquire(t2.getTransactionContext(), resource, LockType.X);

                // If we reach here without being killed, something is wrong
                t2Result.set("acquired lock — was not killed");
                t2.rollback();
                t2.close();
            } catch (Exception e) {
                t2Result.set("exception: " + e.getClass().getSimpleName());
            } finally {
                t2Finished.countDown();
            }
        });
        t2Thread.start();

        assertTrue("T2 should be ready", t2Blocked.await(5, TimeUnit.SECONDS));
        Thread.sleep(500); // let T2 enter block()

        long t2TransNum = t2num.get();
        assertTrue("T2 should have a valid transaction number", t2TransNum > 0);

        // Verify T2 is in registry and has lock info before kill
        assertNotNull("T2 should be in registry", db.transactionRegistry.get(t2TransNum));
        String beforeKill = lockManager.getAllLockInfo();
        assertTrue("lock state should mention T2 before kill",
                beforeKill.contains(String.valueOf(t2TransNum)));

        // DDA kills T2
        db.rollbackTransaction(t2TransNum);

        // T2 should finish shortly
        assertTrue("T2 should finish after kill", t2Finished.await(5, TimeUnit.SECONDS));

        // T2 should have received an exception (not acquired the lock)
        assertTrue("T2 result should mention exception, was: " + t2Result.get(),
                t2Result.get().contains("exception"));

        // Registry should be clean for T2
        assertNull("T2 should be removed from registry", db.transactionRegistry.get(t2TransNum));

        // T1 should still be functional
        String afterKill = lockManager.getAllLockInfo();
        assertTrue("T1 should still hold lock after kill",
                afterKill.contains("T" + t1.getTransNum()));

        // Clean up T1
        t1.rollback();
        t1.close();
        t2Thread.join(5000);
    }

    /**
     * Kill a transaction, then verify other blocked waiters can proceed.
     * T1: holds X on resource
     * T2: blocked on X
     * T3: blocked on X
     * Kill T2 → T3 should still be blocked on T1 (T2's queue entry removed, T3's stays)
     * Kill T1 → T3 should get the lock
     */
    @Test
    public void testKillVictimOtherWaitersUnaffected() throws Exception {
        db = createDatabase();
        LockManager lockManager = db.getLockManager();
        ResourceName resource = new ResourceName("test_resource_2");

        // T1: hold X lock
        Transaction t1 = db.beginTransaction();
        lockManager.acquire(t1.getTransactionContext(), resource, LockType.X);

        // T2: block on X (will be killed)
        CountDownLatch t2Blocked = new CountDownLatch(1);
        CountDownLatch t2Done = new CountDownLatch(1);
        AtomicLong t2num = new AtomicLong();
        AtomicReference<String> t2Result = new AtomicReference<>();

        Thread t2Thread = new Thread(() -> {
            try {
                Transaction t2 = db.beginTransaction();
                t2num.set(t2.getTransNum());
                t2Blocked.countDown();
                Thread.sleep(200);
                lockManager.acquire(t2.getTransactionContext(), resource, LockType.X);
                t2.rollback(); t2.close();
            } catch (Exception e) {
                t2Result.set("killed: " + e.getClass().getSimpleName());
            } finally {
                t2Done.countDown();
            }
        });
        t2Thread.start();

        // T3: also block on X
        CountDownLatch t3Blocked = new CountDownLatch(1);
        CountDownLatch t3Done = new CountDownLatch(1);
        AtomicLong t3num = new AtomicLong();
        AtomicReference<String> t3Result = new AtomicReference<>();

        Thread t3Thread = new Thread(() -> {
            try {
                Transaction t3 = db.beginTransaction();
                t3num.set(t3.getTransNum());
                t3Blocked.countDown();
                Thread.sleep(200);
                lockManager.acquire(t3.getTransactionContext(), resource, LockType.X);
                t3Result.set("acquired lock after T1 released");
                t3.rollback(); t3.close();
            } catch (Exception e) {
                t3Result.set("exception: " + e.getClass().getSimpleName());
            } finally {
                t3Done.countDown();
            }
        });
        t3Thread.start();

        // Wait for both to be ready
        assertTrue(t2Blocked.await(5, TimeUnit.SECONDS));
        assertTrue(t3Blocked.await(5, TimeUnit.SECONDS));
        Thread.sleep(500);

        // Kill T2 (one of the waiters)
        db.rollbackTransaction(t2num.get());
        assertTrue(t2Done.await(5, TimeUnit.SECONDS));

        // T3 should still be blocked (T1 still holds the lock)
        assertFalse("T3 should still be blocked", t3Done.await(1, TimeUnit.SECONDS));

        // Kill T1 (the holder) → T3 should get the lock
        db.rollbackTransaction(t1.getTransNum());
        assertTrue("T3 should finish after T1 is killed", t3Done.await(5, TimeUnit.SECONDS));
        assertTrue("T3 should have acquired the lock",
                t3Result.get() != null && t3Result.get().contains("acquired"));

        t1.close();
        t2Thread.join(5000);
        t3Thread.join(5000);
    }

    // =========================================================================
    // End-to-End: Deadlock + Kill
    // =========================================================================

    /**
     * Simulate a classic deadlock: T1 holds A, wants B; T2 holds B, wants A.
     * DDA kills T1 → T2 should proceed.
     */
    @Test
    public void testE2EDeadlockAndKill() throws Exception {
        db = createDatabase();
        LockManager lockManager = db.getLockManager();
        ResourceName resourceA = new ResourceName("resource_A");
        ResourceName resourceB = new ResourceName("resource_B");

        // T1: acquire A, then B
        // T2: acquire B, then A
        // Deadlock: T1 holds A+waits B, T2 holds B+waits A

        CountDownLatch t1HoldsA = new CountDownLatch(1);
        CountDownLatch t2HoldsB = new CountDownLatch(1);
        CountDownLatch t1Done = new CountDownLatch(1);
        CountDownLatch t2Done = new CountDownLatch(1);
        AtomicLong t1num = new AtomicLong();
        AtomicLong t2num = new AtomicLong();
        AtomicReference<String> t1Result = new AtomicReference<>();
        AtomicReference<String> t2Result = new AtomicReference<>();

        Thread t1Thread = new Thread(() -> {
            try {
                Transaction t1 = db.beginTransaction();
                t1num.set(t1.getTransNum());
                lockManager.acquire(t1.getTransactionContext(), resourceA, LockType.X);
                t1HoldsA.countDown();
                t2HoldsB.await(5, TimeUnit.SECONDS);
                Thread.sleep(200);
                lockManager.acquire(t1.getTransactionContext(), resourceB, LockType.X);
                t1Result.set("got B — deadlock was broken");
                t1.rollback(); t1.close();
            } catch (Exception e) {
                t1Result.set("killed: " + e.getClass().getSimpleName());
            } finally {
                t1Done.countDown();
            }
        });

        Thread t2Thread = new Thread(() -> {
            try {
                Transaction t2 = db.beginTransaction();
                t2num.set(t2.getTransNum());
                lockManager.acquire(t2.getTransactionContext(), resourceB, LockType.X);
                t2HoldsB.countDown();
                t1HoldsA.await(5, TimeUnit.SECONDS);
                Thread.sleep(200);
                lockManager.acquire(t2.getTransactionContext(), resourceA, LockType.X);
                t2Result.set("got A — deadlock was broken");
                t2.rollback(); t2.close();
            } catch (Exception e) {
                t2Result.set("killed: " + e.getClass().getSimpleName());
            } finally {
                t2Done.countDown();
            }
        });

        t1Thread.start();
        t2Thread.start();

        // Wait for deadlock to form (both hold one lock and are blocked on the other)
        assertTrue(t1HoldsA.await(5, TimeUnit.SECONDS));
        assertTrue(t2HoldsB.await(5, TimeUnit.SECONDS));
        Thread.sleep(1000); // let both enter block()

        // Verify deadlock exists: T1 waits for B (held by T2), T2 waits for A (held by T1)
        String lockInfo = lockManager.getAllLockInfo();
        assertTrue("T1 should be in lock state", lockInfo.contains("T" + t1num.get()));
        assertTrue("T2 should be in lock state", lockInfo.contains("T" + t2num.get()));

        // DDA kills T1
        db.rollbackTransaction(t1num.get());

        // T1 should finish (killed), T2 should get lock A and finish
        assertTrue("T1 should finish", t1Done.await(5, TimeUnit.SECONDS));
        assertTrue("T2 should finish", t2Done.await(5, TimeUnit.SECONDS));

        assertTrue("T1 should have been killed", t1Result.get().contains("killed"));
        assertTrue("T2 should have acquired lock A", t2Result.get().contains("got A"));

        t1Thread.join(5000);
        t2Thread.join(5000);
    }
}