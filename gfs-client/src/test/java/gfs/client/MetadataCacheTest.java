package gfs.client;

import gfs.common.ChunkHandle;
import gfs.common.ChunkVersion;
import gfs.common.ChunkserverId;
import gfs.common.LocatedChunk;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataCacheTest {

    private MutableClock clock;
    private MetadataCache cache;

    static class MutableClock extends Clock {
        private Instant now;

        public MutableClock(Instant now) {
            this.now = now;
        }

        public void advanceMillis(long millis) {
            now = now.plusMillis(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-05-24T00:00:00Z"));
        cache = new MetadataCache(clock, 5000L); // 5 seconds TTL
    }

    @Test
    void testCachePutAndGet() {
        LocatedChunk chunk0 = new LocatedChunk(
                new ChunkHandle(101L),
                ChunkVersion.ZERO,
                List.of(ChunkserverId.of("127.0.0.1", 8001))
        );
        LocatedChunk chunk1 = new LocatedChunk(
                new ChunkHandle(102L),
                ChunkVersion.ZERO,
                List.of(ChunkserverId.of("127.0.0.1", 8002))
        );

        // Put a range of chunks
        cache.put("/file1", 0, List.of(chunk0, chunk1));

        // Retrieve by path and index range
        Optional<List<LocatedChunk>> retrieved = cache.get("/file1", 0, 1);
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get()).containsExactly(chunk0, chunk1);

        // Retrieve individual by handle
        Optional<LocatedChunk> gotHandle0 = cache.get(new ChunkHandle(101L));
        assertThat(gotHandle0).isPresent();
        assertThat(gotHandle0.get()).isEqualTo(chunk0);

        // Advance clock within TTL
        clock.advanceMillis(4000L);
        assertThat(cache.get("/file1", 0, 1)).isPresent();

        // Advance clock past TTL
        clock.advanceMillis(2000L); // Total 6s (TTL is 5s)
        assertThat(cache.get("/file1", 0, 1)).isEmpty();
        assertThat(cache.get(new ChunkHandle(101L))).isEmpty();
    }

    @Test
    void testCacheInvalidateByPath() {
        LocatedChunk chunk0 = new LocatedChunk(
                new ChunkHandle(201L),
                ChunkVersion.ZERO,
                List.of(ChunkserverId.of("127.0.0.1", 8001))
        );

        cache.put("/file2", 0, List.of(chunk0));
        assertThat(cache.get("/file2", 0, 0)).isPresent();
        assertThat(cache.get(new ChunkHandle(201L))).isPresent();

        cache.invalidate("/file2");
        assertThat(cache.get("/file2", 0, 0)).isEmpty();
        assertThat(cache.get(new ChunkHandle(201L))).isEmpty();
    }

    @Test
    void testCacheInvalidateByHandle() {
        LocatedChunk chunk0 = new LocatedChunk(
                new ChunkHandle(301L),
                ChunkVersion.ZERO,
                List.of(ChunkserverId.of("127.0.0.1", 8001))
        );

        cache.put("/file3", 0, List.of(chunk0));
        assertThat(cache.get("/file3", 0, 0)).isPresent();

        cache.invalidate(new ChunkHandle(301L));
        assertThat(cache.get("/file3", 0, 0)).isEmpty();
        assertThat(cache.get(new ChunkHandle(301L))).isEmpty();
    }

    @Test
    void testMergeCacheUpdates() {
        LocatedChunk chunk0 = new LocatedChunk(
                new ChunkHandle(401L),
                ChunkVersion.ZERO,
                List.of(ChunkserverId.of("127.0.0.1", 8001))
        );
        LocatedChunk chunk1 = new LocatedChunk(
                new ChunkHandle(402L),
                ChunkVersion.ZERO,
                List.of(ChunkserverId.of("127.0.0.1", 8002))
        );

        cache.put("/file4", 0, List.of(chunk0));
        assertThat(cache.get("/file4", 0, 0)).isPresent();

        // Incrementally add chunk 1 at index 1
        cache.put("/file4", 1, List.of(chunk1));

        Optional<List<LocatedChunk>> both = cache.get("/file4", 0, 1);
        assertThat(both).isPresent();
        assertThat(both.get()).containsExactly(chunk0, chunk1);
    }
}
