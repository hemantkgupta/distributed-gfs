package gfs.master;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class MutableClock extends Clock {
    private Instant now;
    private final ZoneId zoneId;

    public MutableClock(Instant start) {
        this.now = start;
        this.zoneId = ZoneId.systemDefault();
    }

    public synchronized void setTime(Instant time) {
        this.now = time;
    }

    public synchronized void advanceSeconds(long seconds) {
        this.now = this.now.plusSeconds(seconds);
    }

    public synchronized void advanceMillis(long millis) {
        this.now = this.now.plusMillis(millis);
    }

    @Override
    public synchronized Instant instant() {
        return now;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }
}
