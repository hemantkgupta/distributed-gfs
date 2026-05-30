package gfs.oplog;

import gfs.common.ChunkHandle;
import gfs.common.ChunkVersion;
import gfs.common.ChunkserverId;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class OperationLog implements AutoCloseable {
    private final Path logFile;
    private final List<Path> remoteMirrorDirs;
    private final FileOutputStream logStream;
    private final FileChannel logChannel;
    private final List<FileOutputStream> mirrorStreams = new ArrayList<>();
    private final List<FileChannel> mirrorChannels = new ArrayList<>();

    public OperationLog(Path logFile, List<Path> remoteMirrorDirs) throws IOException {
        this.logFile = logFile;
        this.remoteMirrorDirs = remoteMirrorDirs != null ? remoteMirrorDirs : List.of();

        // Ensure parent directory of logFile exists
        if (logFile.getParent() != null) {
            Files.createDirectories(logFile.getParent());
        }

        // Open main log file in append mode
        this.logStream = new FileOutputStream(logFile.toFile(), true);
        this.logChannel = logStream.getChannel();

        // Open mirror files in append mode
        String fileName = logFile.getFileName().toString();
        for (Path mirrorDir : this.remoteMirrorDirs) {
            Files.createDirectories(mirrorDir);
            Path mirrorFile = mirrorDir.resolve(fileName);
            FileOutputStream fos = new FileOutputStream(mirrorFile.toFile(), true);
            this.mirrorStreams.add(fos);
            this.mirrorChannels.add(fos.getChannel());
        }
    }

    public synchronized void append(LogEntry entry, boolean fsync) throws IOException {
        byte type = getTypeTag(entry);
        byte[] payload = serializeEntry(entry);
        // length is type (1 B) + seq (4 B) + payload length
        int length = 1 + 4 + payload.length;

        // Allocate buffer for length (4 B le) + type (1 B) + seq (4 B) + payload
        ByteBuffer buf = ByteBuffer.allocate(4 + length);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.put(type);
        buf.putInt(entry.sequence());
        buf.put(payload);
        buf.flip();

        // Write to main log file
        ByteBuffer mainBuf = buf.duplicate();
        while (mainBuf.hasRemaining()) {
            logChannel.write(mainBuf);
        }

        // Write to mirror files
        for (FileChannel mirrorChannel : mirrorChannels) {
            ByteBuffer mirrorBuf = buf.duplicate();
            while (mirrorBuf.hasRemaining()) {
                mirrorChannel.write(mirrorBuf);
            }
        }

        if (fsync) {
            flushAndSync();
        }
    }

    public synchronized void flushAndSync() throws IOException {
        logChannel.force(true);
        for (FileChannel mirrorChannel : mirrorChannels) {
            mirrorChannel.force(true);
        }
    }

    public synchronized void replay(Consumer<LogEntry> callback) throws IOException {
        // Flush main channel to make sure everything is on disk before reading
        logChannel.force(true);

        if (!Files.exists(logFile)) {
            return;
        }

        try (InputStream fis = Files.newInputStream(logFile);
             BufferedInputStream bis = new BufferedInputStream(fis);
             DataInputStream dis = new DataInputStream(bis)) {
            while (true) {
                int b1, b2, b3, b4;
                try {
                    b1 = dis.read();
                    if (b1 == -1) {
                        break; // Clean EOF
                    }
                    b2 = dis.read();
                    b3 = dis.read();
                    b4 = dis.read();
                    if ((b2 | b3 | b4) < 0) {
                        throw new EOFException("Unexpected EOF while reading frame length");
                    }
                } catch (EOFException e) {
                    break; // Clean EOF
                }

                int length = b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
                if (length < 5) {
                    throw new IOException("Invalid record length: " + length);
                }

                int type = dis.read();
                if (type == -1) {
                    throw new EOFException("Unexpected EOF while reading message type");
                }

                int seq = dis.readInt();

                int payloadLen = length - 5;
                byte[] payload = new byte[payloadLen];
                dis.readFully(payload);

                LogEntry entry = deserializePayload(type, seq, payload);
                callback.accept(entry);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        IOException firstEx = null;
        try {
            if (logStream != null) {
                logStream.close();
            }
        } catch (IOException e) {
            firstEx = e;
        }
        for (FileOutputStream fos : mirrorStreams) {
            try {
                fos.close();
            } catch (IOException e) {
                if (firstEx == null) {
                    firstEx = e;
                }
            }
        }
        if (firstEx != null) {
            throw firstEx;
        }
    }

    private static byte[] serializeEntry(LogEntry entry) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(entry.timestamp().toEpochMilli());

            if (entry instanceof LogEntry.CreateFile e) {
                dos.writeUTF(e.path());
            } else if (entry instanceof LogEntry.DeleteFile e) {
                dos.writeUTF(e.path());
            } else if (entry instanceof LogEntry.Mkdir e) {
                dos.writeUTF(e.path());
            } else if (entry instanceof LogEntry.AllocateChunk e) {
                dos.writeUTF(e.path());
                dos.writeLong(e.chunk().id());
                dos.writeInt(e.version().v());
                dos.writeInt(e.replicas().size());
                for (ChunkserverId replica : e.replicas()) {
                    dos.writeUTF(replica.hostPort());
                }
            } else if (entry instanceof LogEntry.SetChunkReplicas e) {
                dos.writeLong(e.chunk().id());
                dos.writeInt(e.replicas().size());
                for (ChunkserverId replica : e.replicas()) {
                    dos.writeUTF(replica.hostPort());
                }
            } else if (entry instanceof LogEntry.GrantLease e) {
                dos.writeLong(e.chunk().id());
                dos.writeUTF(e.primary().hostPort());
                dos.writeLong(e.expiresAt().toEpochMilli());
            } else if (entry instanceof LogEntry.RenewLease e) {
                dos.writeLong(e.chunk().id());
                dos.writeLong(e.newExpiresAt().toEpochMilli());
            } else if (entry instanceof LogEntry.RevokeLease e) {
                dos.writeLong(e.chunk().id());
            } else if (entry instanceof LogEntry.MarkReplicaStale e) {
                dos.writeLong(e.chunk().id());
                dos.writeUTF(e.stale().hostPort());
            } else if (entry instanceof LogEntry.DeleteChunk e) {
                dos.writeLong(e.chunk().id());
            } else {
                throw new IOException("Unknown LogEntry implementation: " + entry.getClass());
            }
        }
        return baos.toByteArray();
    }

    private static byte getTypeTag(LogEntry entry) {
        if (entry instanceof LogEntry.CreateFile) return 1;
        if (entry instanceof LogEntry.DeleteFile) return 2;
        if (entry instanceof LogEntry.Mkdir) return 3;
        if (entry instanceof LogEntry.AllocateChunk) return 4;
        if (entry instanceof LogEntry.SetChunkReplicas) return 5;
        if (entry instanceof LogEntry.GrantLease) return 6;
        if (entry instanceof LogEntry.RenewLease) return 7;
        if (entry instanceof LogEntry.RevokeLease) return 8;
        if (entry instanceof LogEntry.MarkReplicaStale) return 9;
        if (entry instanceof LogEntry.DeleteChunk) return 10;
        throw new IllegalArgumentException("Unknown LogEntry implementation: " + entry.getClass());
    }

    private static LogEntry deserializePayload(int type, int seq, byte[] payload) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(payload);
             DataInputStream dis = new DataInputStream(bais)) {

            long ts = dis.readLong();
            Instant timestamp = Instant.ofEpochMilli(ts);

            switch (type) {
                case 1 -> {
                    String path = dis.readUTF();
                    return new LogEntry.CreateFile(seq, timestamp, path);
                }
                case 2 -> {
                    String path = dis.readUTF();
                    return new LogEntry.DeleteFile(seq, timestamp, path);
                }
                case 3 -> {
                    String path = dis.readUTF();
                    return new LogEntry.Mkdir(seq, timestamp, path);
                }
                case 4 -> {
                    String path = dis.readUTF();
                    long chunkId = dis.readLong();
                    int ver = dis.readInt();
                    int repCount = dis.readInt();
                    Set<ChunkserverId> replicas = new HashSet<>();
                    for (int i = 0; i < repCount; i++) {
                        replicas.add(new ChunkserverId(dis.readUTF()));
                    }
                    return new LogEntry.AllocateChunk(seq, timestamp, path, new ChunkHandle(chunkId), new ChunkVersion(ver), replicas);
                }
                case 5 -> {
                    long chunkId = dis.readLong();
                    int repCount = dis.readInt();
                    Set<ChunkserverId> replicas = new HashSet<>();
                    for (int i = 0; i < repCount; i++) {
                        replicas.add(new ChunkserverId(dis.readUTF()));
                    }
                    return new LogEntry.SetChunkReplicas(seq, timestamp, new ChunkHandle(chunkId), replicas);
                }
                case 6 -> {
                    long chunkId = dis.readLong();
                    String primary = dis.readUTF();
                    long exp = dis.readLong();
                    return new LogEntry.GrantLease(seq, timestamp, new ChunkHandle(chunkId), new ChunkserverId(primary), Instant.ofEpochMilli(exp));
                }
                case 7 -> {
                    long chunkId = dis.readLong();
                    long exp = dis.readLong();
                    return new LogEntry.RenewLease(seq, timestamp, new ChunkHandle(chunkId), Instant.ofEpochMilli(exp));
                }
                case 8 -> {
                    long chunkId = dis.readLong();
                    return new LogEntry.RevokeLease(seq, timestamp, new ChunkHandle(chunkId));
                }
                case 9 -> {
                    long chunkId = dis.readLong();
                    String stale = dis.readUTF();
                    return new LogEntry.MarkReplicaStale(seq, timestamp, new ChunkHandle(chunkId), new ChunkserverId(stale));
                }
                case 10 -> {
                    long chunkId = dis.readLong();
                    return new LogEntry.DeleteChunk(seq, timestamp, new ChunkHandle(chunkId));
                }
                default -> throw new IOException("Unknown log entry type: " + type);
            }
        }
    }
}
