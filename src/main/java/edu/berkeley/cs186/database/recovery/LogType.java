package edu.berkeley.cs186.database.recovery;

/**
 * Enum representing different types of log records in the ARIES recovery protocol.
 * Each log type serves a specific purpose in database recovery operations.
 */
public enum LogType {
    // master log record (stores current checkpoint)
    MASTER,
    // log record for allocating a new page (via the disk space manager)
    ALLOC_PAGE,
    // log record for updating part of a page
    UPDATE_PAGE,
    // log record for freeing a page (via the disk space manager)
    FREE_PAGE,
    // log record for allocating a new partition (via the disk space manager)
    ALLOC_PART,
    // log record for freeing a partition (via the disk space manager)
    FREE_PART,
    // log record for starting a transaction commit
    COMMIT_TRANSACTION,
    // log record for starting a transaction abort
    ABORT_TRANSACTION,
    // log record for after a transaction has completely finished
    END_TRANSACTION,
    // log record for start of a checkpoint
    BEGIN_CHECKPOINT,
    // log record for finishing a checkpoint; there may be multiple of these
    // for a checkpoint
    END_CHECKPOINT,
    // compensation log record for undoing a page alloc
    UNDO_ALLOC_PAGE,
    // compensation log record for undoing a page update
    UNDO_UPDATE_PAGE,
    // compensation log record for undoing a page free
    UNDO_FREE_PAGE,
    // compensation log record for undoing a partition alloc
    UNDO_ALLOC_PART,
    // compensation log record for undoing a partition free
    UNDO_FREE_PART;

    // Cache the values array to avoid repeated array creation
    private static final LogType[] VALUES = LogType.values();
    // Cache min and max values for boundary checks
    private static final int MIN_VALUE = 1;
    private static final int MAX_VALUE = VALUES.length;

    /**
     * Returns the integer value representing this log type.
     * The value is ordinal + 1 to ensure a 1-based index.
     *
     * @return integer value of this log type
     */
    public int getValue() {
        return ordinal() + 1;
    }

    /**
     * Converts an integer value back to the corresponding LogType enum.
     *
     * @param x the integer value to convert
     * @return the corresponding LogType enum
     * @throws IllegalArgumentException if the value is out of range
     */
    public static LogType fromInt(int x) {
        if (x < MIN_VALUE || x > MAX_VALUE) {
            throw new IllegalArgumentException("Unknown TypeId ordinal: " + x);
        }
        return VALUES[x - 1];
    }
}
