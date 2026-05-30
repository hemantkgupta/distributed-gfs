package gfs.common;

import java.util.List;

public record LocatedChunk(
    ChunkHandle handle,
    ChunkVersion version,
    List<ChunkserverId> replicas
) {}
