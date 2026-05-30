package gfs.common;

public record PushBytesRequest(
    ChunkHandle handle,
    Bytes data
) {}
