package cc.whohow.redis.spring.scheduling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.Lock;

public class TryLockRunnable implements Runnable {
    private static final Logger log = LogManager.getLogger();
    private final Runnable runnable;
    private final Lock lock;

    public TryLockRunnable(Runnable runnable, Lock lock) {
        this.runnable = runnable;
        this.lock = lock;
    }

    @Override
    public void run() {
        if (lock.tryLock()) {
            log.trace("lock");
            try {
                runnable.run();
            } finally {
                lock.unlock();
                log.trace("unlock");
            }
        } else {
            log.trace("tryLock skip");
        }
    }
}
