package gfs.common;

import java.util.List;

public record CommitWriteRequest(
    ChunkHandle handle,
    long offset,
    int size,
    List<ChunkserverId> secondaries
) {}
