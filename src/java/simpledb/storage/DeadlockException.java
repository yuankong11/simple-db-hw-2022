package simpledb.storage;

import java.util.HashSet;

/**
 * Exception that is thrown when a deadlock occurs.
 */
public class DeadlockException extends Exception {
    private static final long serialVersionUID = 1L;

    public DeadlockException() {
    }
}
