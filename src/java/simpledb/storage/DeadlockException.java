package simpledb.storage;

/**
 * Exception that is thrown when a deadlock occurs.
 */
public class DeadlockException extends Exception {
    private static final long serialVersionUID = 1L;

    private boolean upgradeDeadlock = false;

    public DeadlockException() {
    }

    public void setUpgradeDeadlock() {
        upgradeDeadlock = true;
    }

    public boolean isUpgradeDeadlock() {
        return upgradeDeadlock;
    }
}
