package gfs.common;

import java.util.List;

public record ReplicateBytes(
    ChunkHandle handle,
    Bytes data,
    List<ChunkserverId> remainingChain
) {}
