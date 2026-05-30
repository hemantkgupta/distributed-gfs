package gfs.common;

import java.util.List;

public record HeartbeatAck(
    List<ChunkHandle> chunksToDelete,
    List<CopyCommand> chunksToCopy
) {}
