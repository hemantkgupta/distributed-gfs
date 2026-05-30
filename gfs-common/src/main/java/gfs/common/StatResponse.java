package gfs.common;

public record StatResponse(
    String path,
    boolean isDirectory,
    long sizeBytes,
    long ctime,
    long mtime
) {}
