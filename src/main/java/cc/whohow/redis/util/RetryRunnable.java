package cc.whohow.redis.util;

public class RetryRunnable implements Runnable {
    private final Runnable runnable;
    private final int retry;

    public RetryRunnable(Runnable runnable, int retry) {
        if (retry < 0) {
            throw new IllegalArgumentException();
        }
        this.runnable = runnable;
        this.retry = retry;
    }

    @Override
    public void run() {
        for (int i = 0; i <= retry; i++) {
            try {
                runnable.run();
            } catch (RuntimeException e) {
                if (i == retry) {
                    throw e;
                }
            }
        }
    }
}
