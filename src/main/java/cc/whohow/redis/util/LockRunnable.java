package cc.whohow.redis.util;

import java.util.concurrent.locks.Lock;

public class LockRunnable implements Runnable {
    private final Runnable runnable;
    private final Lock lock;

    public LockRunnable(Runnable runnable, Lock lock) {
        this.runnable = runnable;
        this.lock = lock;
    }

    @Override
    public void run() {
        if (lock.tryLock()) {
            try {
                runnable.run();
            } finally {
                lock.unlock();
            }
        }
    }
}
