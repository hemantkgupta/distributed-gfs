package gfs.common;

public record ReplicaStateReport(
    ChunkserverId id,
    ChunkHandle handle,
    ChunkVersion version,
    long sizeBytes
) {}
