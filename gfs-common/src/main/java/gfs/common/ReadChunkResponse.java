package gfs.common;

public record ReadChunkResponse(
    boolean success,
    Bytes data,
    String errorMessage
) {}
