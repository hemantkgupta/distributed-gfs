package gfs.common;

import java.util.List;

public record GetChunkLocationsResponse(
    List<LocatedChunk> chunks
) {}
