package gfs.chunkserver;

import gfs.common.ChunkHandle;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32C;

public class ChunkStore {
    private final Path dataDir;

    public ChunkStore(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
    }

    private Path getChunkPath(ChunkHandle h) {
        return dataDir.resolve(h.id() + ".chunk");
    }

    private Path getCrcPath(ChunkHandle h) {
        return dataDir.resolve(h.id() + ".crc");
    }

    public synchronized void createChunk(ChunkHandle h) throws IOException {
        Path chunkPath = getChunkPath(h);
        Path crcPath = getCrcPath(h);
        if (!Files.exists(chunkPath)) {
            Files.createFile(chunkPath);
        }
        if (!Files.exists(crcPath)) {
            Files.createFile(crcPath);
        }
    }

    public synchronized boolean exists(ChunkHandle h) {
        return Files.exists(getChunkPath(h));
    }

    public synchronized long lengthOf(ChunkHandle h) throws IOException {
        Path chunkPath = getChunkPath(h);
        if (!Files.exists(chunkPath)) {
            throw new FileNotFoundException("Chunk file not found: " + h);
        }
        return Files.size(chunkPath);
    }

    public synchronized void delete(ChunkHandle h) throws IOException {
        Path chunkPath = getChunkPath(h);
        Path crcPath = getCrcPath(h);
        Files.deleteIfExists(chunkPath);
        Files.deleteIfExists(crcPath);
    }

    public synchronized byte[] read(ChunkHandle h, long offset, int size) throws IOException {
        Path chunkPath = getChunkPath(h);
        Path crcPath = getCrcPath(h);
        if (!Files.exists(chunkPath)) {
            throw new FileNotFoundException("Chunk file not found: " + h);
        }

        long fileLength = Files.size(chunkPath);
        if (offset < 0 || size < 0 || offset + size > fileLength) {
            throw new EOFException("Read out of bounds: offset=" + offset + ", size=" + size + ", length=" + fileLength);
        }

        // Verify CRC32C for all spanning 64 KB blocks
        long startBlock = offset / 65536L;
        long endBlock = (offset + size - 1) / 65536L;

        try (FileChannel chunkChannel = FileChannel.open(chunkPath, StandardOpenOption.READ);
             FileChannel crcChannel = FileChannel.open(crcPath, StandardOpenOption.READ)) {

            for (long b = startBlock; b <= endBlock; b++) {
                long blockStart = b * 65536L;
                long blockEnd = Math.min(fileLength, (b + 1) * 65536L);
                int blockSize = (int) (blockEnd - blockStart);

                if (blockSize > 0) {
                    byte[] blockData = new byte[blockSize];
                    chunkChannel.position(blockStart);
                    readFully(chunkChannel, blockData);

                    long computedCrc = computeCrc32c(blockData, 0, blockData.length);
                    long storedCrc = readCrc(crcChannel, b);

                    if (computedCrc != storedCrc) {
                        throw new ChecksumMismatchException("Checksum mismatch on block " + b + " for chunk " + h.id() +
                                ". Expected: " + storedCrc + ", Computed: " + computedCrc);
                    }
                }
            }

            // If verification passes, read the requested range
            byte[] result = new byte[size];
            chunkChannel.position(offset);
            readFully(chunkChannel, result);
            return result;
        }
    }

    public synchronized void writeAt(ChunkHandle h, long offset, byte[] bytes) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        Path chunkPath = getChunkPath(h);
        Path crcPath = getCrcPath(h);

        try (FileChannel chunkChannel = FileChannel.open(chunkPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileChannel crcChannel = FileChannel.open(crcPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            long oldLength = chunkChannel.size();

            // Write chunk data
            chunkChannel.position(offset);
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                chunkChannel.write(buf);
            }

            long newLength = chunkChannel.size();

            long startBlock = offset / 65536L;
            long endBlock = bytes.length == 0 ? startBlock : (offset + bytes.length - 1) / 65536L;

            Set<Long> blocksToUpdate = new HashSet<>();
            for (long b = startBlock; b <= endBlock; b++) {
                blocksToUpdate.add(b);
            }

            if (newLength > oldLength) {
                long oldLastBlock = oldLength == 0 ? -1 : (oldLength - 1) / 65536L;
                for (long b = Math.max(0, oldLastBlock); b < startBlock; b++) {
                    if (b * 65536L >= oldLength || (oldLength % 65536L != 0 && b == oldLastBlock)) {
                        blocksToUpdate.add(b);
                    }
                }
            }

            for (long b : blocksToUpdate) {
                updateBlockCrc(chunkChannel, crcChannel, b, newLength);
            }

            // Sync both files to disk
            chunkChannel.force(true);
            crcChannel.force(true);
        }
    }

    public synchronized List<ChunkHandle> listChunks() throws IOException {
        if (!Files.exists(dataDir)) {
            return Collections.emptyList();
        }
        try (var stream = Files.list(dataDir)) {
            return stream
                    .filter(p -> p.toString().endsWith(".chunk"))
                    .map(p -> {
                        String name = p.getFileName().toString();
                        long id = Long.parseLong(name.substring(0, name.length() - 6));
                        return new ChunkHandle(id);
                    })
                    .toList();
        }
    }

    private void updateBlockCrc(FileChannel chunkChannel, FileChannel crcChannel, long b, long fileLength) throws IOException {
        long blockStart = b * 65536L;
        long blockEnd = Math.min(fileLength, (b + 1) * 65536L);
        int blockSize = (int) (blockEnd - blockStart);
        if (blockSize <= 0) {
            return;
        }
        byte[] blockData = new byte[blockSize];
        chunkChannel.position(blockStart);
        readFully(chunkChannel, blockData);
        long crcVal = computeCrc32c(blockData, 0, blockData.length);
        writeCrc(crcChannel, b, crcVal);
    }

    private long readCrc(FileChannel crcChannel, long b) throws IOException {
        if (crcChannel.size() < (b + 1) * 4) {
            throw new IOException("CRC file is missing entry for block " + b);
        }
        ByteBuffer buf = ByteBuffer.allocate(4);
        crcChannel.position(b * 4);
        readFully(crcChannel, buf);
        buf.flip();
        return buf.getInt() & 0xFFFFFFFFL;
    }

    private void writeCrc(FileChannel crcChannel, long b, long crcVal) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt((int) crcVal);
        buf.flip();
        crcChannel.position(b * 4);
        while (buf.hasRemaining()) {
            crcChannel.write(buf);
        }
    }

    private void readFully(FileChannel channel, byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            int read = channel.read(buf);
            if (read == -1) {
                throw new EOFException("Unexpected EOF while reading from channel");
            }
        }
    }

    private void readFully(FileChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int read = channel.read(buf);
            if (read == -1) {
                throw new EOFException("Unexpected EOF while reading from channel");
            }
        }
    }

    private long computeCrc32c(byte[] data, int off, int len) {
        CRC32C crc = new CRC32C();
        crc.update(data, off, len);
        return crc.getValue();
    }

    public static class ChecksumMismatchException extends IOException {
        public ChecksumMismatchException(String message) {
            super(message);
        }
    }
}
