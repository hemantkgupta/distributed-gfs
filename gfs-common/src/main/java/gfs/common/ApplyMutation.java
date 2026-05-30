package gfs.common;

public record ApplyMutation(
    ChunkHandle handle,
    long serial,
    long offset,
    int size
) {}
