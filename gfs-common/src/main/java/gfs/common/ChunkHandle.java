package gfs.common;

import java.util.Objects;

public record ChunkHandle(long id) implements Comparable<ChunkHandle> {
    public static ChunkHandle of(long id) {
        return new ChunkHandle(id);
    }

    @Override
    public int compareTo(ChunkHandle o) {
        return Long.compare(id, o.id);
    }
}
