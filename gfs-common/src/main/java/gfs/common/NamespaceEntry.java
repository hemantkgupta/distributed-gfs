package gfs.common;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record NamespaceEntry(
    String path,
    boolean isDirectory,
    List<ChunkHandle> chunks,     // null if directory
    long sizeBytes,
    Instant ctime,
    Instant mtime,
    Optional<Instant> deletedAt   // for GC
) {}
