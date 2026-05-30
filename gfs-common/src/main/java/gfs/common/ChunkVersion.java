package gfs.common;

public record ChunkVersion(int v) {
    public static final ChunkVersion ZERO = new ChunkVersion(0);

    public ChunkVersion next() {
        return new ChunkVersion(v + 1);
    }
}
