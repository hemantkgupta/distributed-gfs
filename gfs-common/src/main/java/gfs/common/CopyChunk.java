package gfs.common;

public record CopyChunk(
    ChunkHandle chunk,
    ChunkserverId source
) {}
