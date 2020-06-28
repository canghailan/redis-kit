package cc.whohow.redis.util.impl;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.function.BiFunction;

public class DateRange extends Range<Date> {
    public DateRange(ZonedDateTime start, ZonedDateTime stop, Duration step) {
        this(Date.from(start.toInstant()), Date.from(stop.toInstant()), step);
    }

    public DateRange(Date start, Date stop, Duration step) {
        super(start,
                step.toMillis() > 0 ?
                        new MaxBound<>(Date::compareTo, false, stop) :
                        new MinBound<>(Date::compareTo, false, stop),
                new Step(step));
    }

    private static class Step implements BiFunction<Date, Integer, Date> {
        protected final long step;

        public Step(Duration step) {
            this.step = step.toMillis();
        }

        @Override
        public Date apply(Date date, Integer integer) {
            return new Date(date.getTime() + step);
        }
    }
}
