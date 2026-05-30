package gfs.oplog;

import gfs.common.ChunkHandle;
import gfs.common.ChunkMetadata;
import gfs.common.ChunkVersion;
import gfs.common.ChunkserverId;
import gfs.common.LeaseToken;
import gfs.common.NamespaceEntry;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;

public class Checkpoint {
    private static final int VERSION = 1;

    public record CheckpointState(
        int lastSequence,
        List<NamespaceEntry> namespace,
        List<ChunkMetadata> chunks,
        List<LeaseToken> leases
    ) {}

    public static void write(
        Path file,
        int lastSequence,
        Collection<NamespaceEntry> namespace,
        Collection<ChunkMetadata> chunks,
        Collection<LeaseToken> leases
    ) throws IOException {
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmpFile = file.resolveSibling(file.getFileName().toString() + ".tmp");

        try {
            // Prepare section bytes
            byte[] namespaceBytes = serializeNamespace(namespace);
            byte[] chunksBytes = serializeChunks(chunks);
            byte[] leasesBytes = serializeLeases(leases);

            try (FileOutputStream fos = new FileOutputStream(tmpFile.toFile())) {
                DataOutputStream dos = new DataOutputStream(fos);
                dos.writeInt(VERSION);
                dos.writeInt(lastSequence);

                // Write namespace section: length + bytes
                dos.writeInt(namespaceBytes.length);
                dos.write(namespaceBytes);

                // Write chunkMap section: length + bytes
                dos.writeInt(chunksBytes.length);
                dos.write(chunksBytes);

                // Write leases section: length + bytes
                dos.writeInt(leasesBytes.length);
                dos.write(leasesBytes);

                dos.flush();
                // fsync the file descriptor
                fos.getFD().sync();
            }

            // Atomically rename to final target file
            Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            // Clean up tmp file if it still exists (e.g. on error)
            Files.deleteIfExists(tmpFile);
        }
    }

    public static CheckpointState load(Path file) throws IOException {
        if (!Files.exists(file)) {
            throw new FileNotFoundException("Checkpoint file not found: " + file);
        }

        try (InputStream fis = Files.newInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis);
             DataInputStream dis = new DataInputStream(bis)) {

            int version = dis.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported checkpoint version: " + version + ", expected: " + VERSION);
            }

            int lastSequence = dis.readInt();

            // Read namespace section
            int namespaceLen = dis.readInt();
            byte[] namespaceBytes = new byte[namespaceLen];
            dis.readFully(namespaceBytes);
            List<NamespaceEntry> namespace = deserializeNamespace(namespaceBytes);

            // Read chunks section
            int chunksLen = dis.readInt();
            byte[] chunksBytes = new byte[chunksLen];
            dis.readFully(chunksBytes);
            List<ChunkMetadata> chunks = deserializeChunks(chunksBytes);

            // Read leases section
            int leasesLen = dis.readInt();
            byte[] leasesBytes = new byte[leasesLen];
            dis.readFully(leasesBytes);
            List<LeaseToken> leases = deserializeLeases(leasesBytes);

            return new CheckpointState(lastSequence, namespace, chunks, leases);
        }
    }

    private static byte[] serializeNamespace(Collection<NamespaceEntry> namespace) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(namespace.size());
            for (NamespaceEntry entry : namespace) {
                dos.writeUTF(entry.path());
                dos.writeBoolean(entry.isDirectory());

                if (entry.chunks() == null) {
                    dos.writeInt(-1);
                } else {
                    dos.writeInt(entry.chunks().size());
                    for (ChunkHandle h : entry.chunks()) {
                        dos.writeLong(h.id());
                    }
                }

                dos.writeLong(entry.sizeBytes());
                dos.writeLong(entry.ctime().toEpochMilli());
                dos.writeLong(entry.mtime().toEpochMilli());

                if (entry.deletedAt().isPresent()) {
                    dos.writeBoolean(true);
                    dos.writeLong(entry.deletedAt().get().toEpochMilli());
                } else {
                    dos.writeBoolean(false);
                }
            }
        }
        return baos.toByteArray();
    }

    private static byte[] serializeChunks(Collection<ChunkMetadata> chunks) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(chunks.size());
            for (ChunkMetadata chunk : chunks) {
                dos.writeLong(chunk.handle().id());
                dos.writeInt(chunk.version().v());

                dos.writeInt(chunk.replicas().size());
                for (ChunkserverId replica : chunk.replicas()) {
                    dos.writeUTF(replica.hostPort());
                }

                dos.writeLong(chunk.sizeBytes());

                if (chunk.primary().isPresent()) {
                    dos.writeBoolean(true);
                    dos.writeUTF(chunk.primary().get().hostPort());
                } else {
                    dos.writeBoolean(false);
                }

                if (chunk.leaseExpiresAt().isPresent()) {
                    dos.writeBoolean(true);
                    dos.writeLong(chunk.leaseExpiresAt().get().toEpochMilli());
                } else {
                    dos.writeBoolean(false);
                }
            }
        }
        return baos.toByteArray();
    }

    private static byte[] serializeLeases(Collection<LeaseToken> leases) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(leases.size());
            for (LeaseToken lease : leases) {
                dos.writeLong(lease.chunk().id());
                dos.writeUTF(lease.primary().hostPort());
                dos.writeLong(lease.grantedAt().toEpochMilli());
                dos.writeLong(lease.expiresAt().toEpochMilli());
            }
        }
        return baos.toByteArray();
    }

    private static List<NamespaceEntry> deserializeNamespace(byte[] bytes) throws IOException {
        List<NamespaceEntry> list = new ArrayList<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bais)) {

            int size = dis.readInt();
            for (int i = 0; i < size; i++) {
                String path = dis.readUTF();
                boolean isDirectory = dis.readBoolean();

                int chunksSize = dis.readInt();
                List<ChunkHandle> chunks = null;
                if (chunksSize >= 0) {
                    chunks = new ArrayList<>(chunksSize);
                    for (int j = 0; j < chunksSize; j++) {
                        chunks.add(new ChunkHandle(dis.readLong()));
                    }
                }

                long sizeBytes = dis.readLong();
                Instant ctime = Instant.ofEpochMilli(dis.readLong());
                Instant mtime = Instant.ofEpochMilli(dis.readLong());

                Optional<Instant> deletedAt = Optional.empty();
                if (dis.readBoolean()) {
                    deletedAt = Optional.of(Instant.ofEpochMilli(dis.readLong()));
                }

                list.add(new NamespaceEntry(path, isDirectory, chunks, sizeBytes, ctime, mtime, deletedAt));
            }
        }
        return list;
    }

    private static List<ChunkMetadata> deserializeChunks(byte[] bytes) throws IOException {
        List<ChunkMetadata> list = new ArrayList<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bais)) {

            int size = dis.readInt();
            for (int i = 0; i < size; i++) {
                ChunkHandle handle = new ChunkHandle(dis.readLong());
                ChunkVersion version = new ChunkVersion(dis.readInt());

                int replicaCount = dis.readInt();
                Set<ChunkserverId> replicas = new HashSet<>(replicaCount);
                for (int j = 0; j < replicaCount; j++) {
                    replicas.add(new ChunkserverId(dis.readUTF()));
                }

                long sizeBytes = dis.readLong();

                Optional<ChunkserverId> primary = Optional.empty();
                if (dis.readBoolean()) {
                    primary = Optional.of(new ChunkserverId(dis.readUTF()));
                }

                Optional<Instant> leaseExpiresAt = Optional.empty();
                if (dis.readBoolean()) {
                    leaseExpiresAt = Optional.of(Instant.ofEpochMilli(dis.readLong()));
                }

                list.add(new ChunkMetadata(handle, version, replicas, sizeBytes, primary, leaseExpiresAt));
            }
        }
        return list;
    }

    private static List<LeaseToken> deserializeLeases(byte[] bytes) throws IOException {
        List<LeaseToken> list = new ArrayList<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bais)) {

            int size = dis.readInt();
            for (int i = 0; i < size; i++) {
                ChunkHandle chunk = new ChunkHandle(dis.readLong());
                ChunkserverId primary = new ChunkserverId(dis.readUTF());
                Instant grantedAt = Instant.ofEpochMilli(dis.readLong());
                Instant expiresAt = Instant.ofEpochMilli(dis.readLong());

                list.add(new LeaseToken(chunk, primary, grantedAt, expiresAt));
            }
        }
        return list;
    }
}
