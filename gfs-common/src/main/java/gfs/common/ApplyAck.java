package gfs.common;

public record ApplyAck(
    ChunkHandle handle,
    long serial,
    boolean success,
    String errorMessage
) {}
