package gfs.common;

public record CopyCommand(
    ChunkHandle handle,
    ChunkserverId source
) {}
