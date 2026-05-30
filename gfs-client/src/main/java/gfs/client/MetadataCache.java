package gfs.client;

import gfs.common.ChunkHandle;
import gfs.common.LocatedChunk;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataCache {
    private final Clock clock;
    private final long ttlMillis;

    private final ConcurrentHashMap<String, List<LocatedChunkEntry>> fileCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, LocatedChunkEntry> handleCache = new ConcurrentHashMap<>();

    public record LocatedChunkEntry(LocatedChunk locatedChunk, Instant expiresAt) {
        public boolean isExpired(Instant now) {
            return now.isAfter(expiresAt);
        }
    }

    public MetadataCache(Clock clock, long ttlMillis) {
        this.clock = clock;
        this.ttlMillis = ttlMillis;
    }

    public MetadataCache(Clock clock) {
        this(clock, 60_000L); // 60 seconds default
    }

    public synchronized void put(String path, int startIdx, List<LocatedChunk> chunks) {
        Instant expiresAt = clock.instant().plusMillis(ttlMillis);
        List<LocatedChunkEntry> existing = fileCache.get(path);
        List<LocatedChunkEntry> updated;
        if (existing == null) {
            updated = new ArrayList<>();
        } else {
            boolean hasExpired = existing.stream().anyMatch(e -> e != null && e.isExpired(clock.instant()));
            if (hasExpired) {
                updated = new ArrayList<>();
            } else {
                updated = new ArrayList<>(existing);
            }
        }

        while (updated.size() < startIdx + chunks.size()) {
            updated.add(null);
        }

        for (int i = 0; i < chunks.size(); i++) {
            LocatedChunk chunk = chunks.get(i);
            LocatedChunkEntry entry = new LocatedChunkEntry(chunk, expiresAt);
            updated.set(startIdx + i, entry);
            handleCache.put(chunk.handle(), entry);
        }
        fileCache.put(path, updated);
    }

    public synchronized void put(ChunkHandle handle, LocatedChunk chunk) {
        Instant expiresAt = clock.instant().plusMillis(ttlMillis);
        LocatedChunkEntry entry = new LocatedChunkEntry(chunk, expiresAt);
        handleCache.put(handle, entry);
    }

    public synchronized Optional<List<LocatedChunk>> get(String path, int startIdx, int endIdx) {
        List<LocatedChunkEntry> entries = fileCache.get(path);
        if (entries == null) {
            return Optional.empty();
        }
        if (entries.size() <= endIdx) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        List<LocatedChunk> result = new ArrayList<>();
        for (int i = startIdx; i <= endIdx; i++) {
            LocatedChunkEntry entry = entries.get(i);
            if (entry == null || entry.isExpired(now)) {
                return Optional.empty();
            }
            result.add(entry.locatedChunk());
        }
        return Optional.of(result);
    }

    public synchronized Optional<LocatedChunk> get(ChunkHandle handle) {
        LocatedChunkEntry entry = handleCache.get(handle);
        if (entry == null) {
            return Optional.empty();
        }
        if (entry.isExpired(clock.instant())) {
            invalidate(handle);
            return Optional.empty();
        }
        return Optional.of(entry.locatedChunk());
    }

    public synchronized void invalidate(String path) {
        List<LocatedChunkEntry> entries = fileCache.remove(path);
        if (entries != null) {
            for (LocatedChunkEntry entry : entries) {
                if (entry != null) {
                    handleCache.remove(entry.locatedChunk().handle());
                }
            }
        }
    }

    public synchronized void invalidate(ChunkHandle handle) {
        handleCache.remove(handle);
        fileCache.entrySet().removeIf(entry -> {
            boolean contains = entry.getValue().stream()
                    .anyMatch(c -> c != null && c.locatedChunk().handle().equals(handle));
            return contains;
        });
    }

    public synchronized void clear() {
        fileCache.clear();
        handleCache.clear();
    }
}
