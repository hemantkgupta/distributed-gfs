package gfs.common;

public record ReadChunkRequest(
    ChunkHandle handle,
    long offset,
    int size
) {}
