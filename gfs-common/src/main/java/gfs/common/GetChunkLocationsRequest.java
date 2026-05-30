package gfs.common;

public record GetChunkLocationsRequest(
    String path,
    long offset,
    long size
) {}
