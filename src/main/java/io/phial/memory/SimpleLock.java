package io.phial.memory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

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

    public boolean tryLock() {
        return this.state == 0 && STATE.compareAndSet(this, 0, 1);
    }

    public void unlock() {
        STATE.compareAndSet(this, 1, 0);
    }
}
