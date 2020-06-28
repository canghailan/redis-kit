package cc.whohow.redis.util.impl;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.function.BiFunction;

public class DateRange extends Range<Date> {
    public DateRange(ZonedDateTime start, ZonedDateTime stop, Duration step) {
        this(Date.from(start.toInstant()), Date.from(stop.toInstant()), step);
    }

    public DateRange(Date start, Date stop, Duration increment) {
        super(start,
                increment.toMillis() > 0 ?
                        new MaxBound<>(Date::compareTo, false, stop) :
                        new MinBound<>(Date::compareTo, false, stop),
                new Increment(increment));
    }

    private static class Increment implements BiFunction<Date, Integer, Date> {
        protected final long increment;

        public Increment(Duration increment) {
            this.increment = increment.toMillis();
        }

        @Override
        public Date apply(Date date, Integer index) {
            return new Date(date.getTime() + increment);
        }
    }
}
