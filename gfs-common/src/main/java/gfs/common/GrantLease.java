package gfs.common;

public record GrantLease(
    ChunkHandle chunk,
    ChunkserverId primary,
    long grantedAtEpochMillis,
    long expiresAtEpochMillis
) {}
