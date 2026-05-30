package gfs.common;

public record CommitResponse(
    boolean success,
    long offset,
    String errorMessage
) {}
