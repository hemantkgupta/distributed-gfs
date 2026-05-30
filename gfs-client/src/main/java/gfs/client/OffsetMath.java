package gfs.client;

import java.util.ArrayList;
import java.util.List;

public final class OffsetMath {
    public static final long CHUNK_SIZE = 64L * 1024 * 1024; // 64 MB

    private OffsetMath() {}

    public static int chunkIndex(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        return (int) (offset / CHUNK_SIZE);
    }

    public static int offsetInChunk(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        return (int) (offset % CHUNK_SIZE);
    }

    public static List<Integer> bytesNeededAcrossChunks(long offset, long size) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative: " + size);
        }
        List<Integer> result = new ArrayList<>();
        if (size == 0) {
            return result;
        }

        long currentOffset = offset;
        long remaining = size;
        while (remaining > 0) {
            int offInChunk = offsetInChunk(currentOffset);
            long chunkRemaining = CHUNK_SIZE - offInChunk;
            int currentSize = (int) Math.min(remaining, chunkRemaining);
            result.add(currentSize);

            currentOffset += currentSize;
            remaining -= currentSize;
        }
        return result;
    }
}
