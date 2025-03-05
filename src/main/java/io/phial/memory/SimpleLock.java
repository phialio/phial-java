package io.phial.memory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

public class SimpleLock {
    private static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(SimpleLock.class, "state", int.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private int state;

    public static void parkNanos() {
        LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(50, 150));
    }

    public void lock() {
        while (!this.tryLock()) {
            SimpleLock.parkNanos();
        }
    }

    public boolean tryLock() {
        return this.state == 0 && STATE.compareAndSet(this, 0, 1);
    }

    public boolean tryLockOrPark() {
        boolean result = this.tryLock();
        if (!result) {
            SimpleLock.parkNanos();
        }
        return result;
    }

    public void unlock() {
        STATE.compareAndSet(this, 1, 0);
    }
}
