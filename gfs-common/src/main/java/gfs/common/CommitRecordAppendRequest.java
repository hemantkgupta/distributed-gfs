package gfs.common;

import java.util.List;

public record CommitRecordAppendRequest(
    ChunkHandle handle,
    List<ChunkserverId> secondaries
) {}
