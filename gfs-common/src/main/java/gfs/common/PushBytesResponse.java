package gfs.common;

public record PushBytesResponse(
    boolean success,
    String errorMessage
) {}
