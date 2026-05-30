package gfs.common;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

public record ChunkMetadata(
    ChunkHandle handle,
    ChunkVersion version,
    Set<ChunkserverId> replicas,
    long sizeBytes,               // current length, <= 64 MB
    Optional<ChunkserverId> primary,
    Optional<Instant> leaseExpiresAt
) {}
