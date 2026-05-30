package gfs.common;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class WireCodec {

    private WireCodec() {}

    public static void encode(WireMessage msg, OutputStream out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream tempOut = new DataOutputStream(baos);
        
        serializePayload(msg.type(), msg.payload(), tempOut);
        tempOut.flush();
        byte[] payloadBytes = baos.toByteArray();
        
        // Total frame length: 4 (length itself) + 1 (type) + 4 (reqId) + payloadBytes.length
        int length = 4 + 1 + 4 + payloadBytes.length;
        
        DataOutputStream dataOut = new DataOutputStream(out);
        dataOut.writeInt(length);
        dataOut.writeByte(msg.type().getCode());
        dataOut.writeInt(msg.reqId());
        dataOut.write(payloadBytes);
        dataOut.flush();
    }

    public static WireMessage decode(InputStream in) throws IOException {
        DataInputStream dataIn = new DataInputStream(in);
        int length;
        try {
            length = dataIn.readInt();
        } catch (EOFException e) {
            throw e;
        }
        if (length < 9) {
            throw new IOException("Invalid frame length: " + length);
        }
        byte typeCode = dataIn.readByte();
        int reqId = dataIn.readInt();
        
        MessageType type = MessageType.fromCode(typeCode);
        
        int payloadLength = length - 9;
        byte[] payloadBytes = new byte[payloadLength];
        dataIn.readFully(payloadBytes);
        
        ByteArrayInputStream bais = new ByteArrayInputStream(payloadBytes);
        DataInputStream tempIn = new DataInputStream(bais);
        Object payload = deserializePayload(type, tempIn);
        
        return new WireMessage(reqId, type, payload);
    }

    private static void serializePayload(MessageType type, Object payload, DataOutputStream out) throws IOException {
        switch (type) {
            case GET_CHUNK_LOCATIONS_REQUEST -> {
                var req = (GetChunkLocationsRequest) payload;
                writeString(out, req.path());
                out.writeLong(req.offset());
                out.writeLong(req.size());
            }
            case GET_CHUNK_LOCATIONS_RESPONSE -> {
                var res = (GetChunkLocationsResponse) payload;
                out.writeByte(res.chunks().size());
                for (var chunk : res.chunks()) {
                    out.writeLong(chunk.handle().id());
                    out.writeInt(chunk.version().v());
                    out.writeByte(chunk.replicas().size());
                    for (var replica : chunk.replicas()) {
                        writeChunkserverId(out, replica);
                    }
                }
            }
            case CREATE_FILE_REQUEST -> {
                var req = (CreateFileRequest) payload;
                writeString(out, req.path());
            }
            case CREATE_FILE_RESPONSE -> {
                var res = (CreateFileResponse) payload;
                out.writeBoolean(res.success());
            }
            case DELETE_FILE_REQUEST -> {
                var req = (DeleteFileRequest) payload;
                writeString(out, req.path());
            }
            case DELETE_FILE_RESPONSE -> {
                var res = (DeleteFileResponse) payload;
                out.writeBoolean(res.success());
            }
            case MKDIR_REQUEST -> {
                var req = (MkdirRequest) payload;
                writeString(out, req.path());
            }
            case MKDIR_RESPONSE -> {
                var res = (MkdirResponse) payload;
                out.writeBoolean(res.success());
            }
            case STAT_REQUEST -> {
                var req = (StatRequest) payload;
                writeString(out, req.path());
            }
            case STAT_RESPONSE -> {
                var res = (StatResponse) payload;
                writeString(out, res.path());
                out.writeBoolean(res.isDirectory());
                out.writeLong(res.sizeBytes());
                out.writeLong(res.ctime());
                out.writeLong(res.mtime());
            }
            case ERROR_RESPONSE -> {
                var res = (ErrorResponse) payload;
                writeString(out, res.errorMessage());
            }
            case READ_CHUNK_REQUEST -> {
                var req = (ReadChunkRequest) payload;
                out.writeLong(req.handle().id());
                out.writeLong(req.offset());
                out.writeInt(req.size());
            }
            case READ_CHUNK_RESPONSE -> {
                var res = (ReadChunkResponse) payload;
                out.writeBoolean(res.success());
                writeBytes(out, res.data());
                writeString(out, res.errorMessage());
            }
            case PUSH_BYTES_REQUEST -> {
                var req = (PushBytesRequest) payload;
                out.writeLong(req.handle().id());
                writeBytes(out, req.data());
            }
            case PUSH_BYTES_RESPONSE -> {
                var res = (PushBytesResponse) payload;
                out.writeBoolean(res.success());
                writeString(out, res.errorMessage());
            }
            case COMMIT_WRITE_REQUEST -> {
                var req = (CommitWriteRequest) payload;
                out.writeLong(req.handle().id());
                out.writeLong(req.offset());
                out.writeInt(req.size());
                out.writeByte(req.secondaries().size());
                for (var sec : req.secondaries()) {
                    writeChunkserverId(out, sec);
                }
            }
            case COMMIT_RECORD_APPEND_REQUEST -> {
                var req = (CommitRecordAppendRequest) payload;
                out.writeLong(req.handle().id());
                out.writeByte(req.secondaries().size());
                for (var sec : req.secondaries()) {
                    writeChunkserverId(out, sec);
                }
            }
            case COMMIT_RESPONSE -> {
                var res = (CommitResponse) payload;
                out.writeBoolean(res.success());
                out.writeLong(res.offset());
                writeString(out, res.errorMessage());
            }
            case REPLICATE_BYTES -> {
                var req = (ReplicateBytes) payload;
                out.writeLong(req.handle().id());
                writeBytes(out, req.data());
                out.writeByte(req.remainingChain().size());
                for (var node : req.remainingChain()) {
                    writeChunkserverId(out, node);
                }
            }
            case APPLY_MUTATION -> {
                var req = (ApplyMutation) payload;
                out.writeLong(req.handle().id());
                out.writeLong(req.serial());
                out.writeLong(req.offset());
                out.writeInt(req.size());
            }
            case APPLY_ACK -> {
                var res = (ApplyAck) payload;
                out.writeLong(res.handle().id());
                out.writeLong(res.serial());
                out.writeBoolean(res.success());
                writeString(out, res.errorMessage());
            }
            case HEARTBEAT -> {
                var hb = (Heartbeat) payload;
                writeChunkserverId(out, hb.id());
                out.writeLong(hb.diskFreeBytes());
                out.writeInt(hb.numChunksHeld());
                out.writeBoolean(hb.leaseRenewalRequested());
                
                out.writeInt(hb.recentlyAddedOrChangedChunks().size());
                for (var h : hb.recentlyAddedOrChangedChunks()) {
                    out.writeLong(h.id());
                }
                
                out.writeInt(hb.chunkSizes().size());
                for (var entry : hb.chunkSizes().entrySet()) {
                    out.writeLong(entry.getKey().id());
                    out.writeLong(entry.getValue());
                }
                
                out.writeInt(hb.staleReplicaReports().size());
                for (var h : hb.staleReplicaReports()) {
                    out.writeLong(h.id());
                }
            }
            case REPLICA_STATE_REPORT -> {
                var rep = (ReplicaStateReport) payload;
                writeChunkserverId(out, rep.id());
                out.writeLong(rep.handle().id());
                out.writeInt(rep.version().v());
                out.writeLong(rep.sizeBytes());
            }
            case GRANT_LEASE -> {
                var req = (GrantLease) payload;
                out.writeLong(req.chunk().id());
                writeChunkserverId(out, req.primary());
                out.writeLong(req.grantedAtEpochMillis());
                out.writeLong(req.expiresAtEpochMillis());
            }
            case REVOKE_LEASE -> {
                var req = (RevokeLease) payload;
                out.writeLong(req.chunk().id());
            }
            case COPY_CHUNK -> {
                var req = (CopyChunk) payload;
                out.writeLong(req.chunk().id());
                writeChunkserverId(out, req.source());
            }
            case DELETE_CHUNK -> {
                var req = (DeleteChunk) payload;
                out.writeLong(req.chunk().id());
            }
            case HEARTBEAT_ACK -> {
                var ack = (HeartbeatAck) payload;
                out.writeInt(ack.chunksToDelete().size());
                for (var h : ack.chunksToDelete()) {
                    out.writeLong(h.id());
                }
                
                out.writeInt(ack.chunksToCopy().size());
                for (var cmd : ack.chunksToCopy()) {
                    out.writeLong(cmd.handle().id());
                    writeChunkserverId(out, cmd.source());
                }
            }
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        }
    }

    private static Object deserializePayload(MessageType type, DataInputStream in) throws IOException {
        switch (type) {
            case GET_CHUNK_LOCATIONS_REQUEST -> {
                String path = readString(in);
                long offset = in.readLong();
                long size = in.readLong();
                return new GetChunkLocationsRequest(path, offset, size);
            }
            case GET_CHUNK_LOCATIONS_RESPONSE -> {
                int chunkCount = in.readByte() & 0xFF;
                List<LocatedChunk> chunks = new java.util.ArrayList<>(chunkCount);
                for (int i = 0; i < chunkCount; i++) {
                    ChunkHandle handle = new ChunkHandle(in.readLong());
                    ChunkVersion version = new ChunkVersion(in.readInt());
                    int replicaCount = in.readByte() & 0xFF;
                    List<ChunkserverId> replicas = new java.util.ArrayList<>(replicaCount);
                    for (int j = 0; j < replicaCount; j++) {
                        replicas.add(readChunkserverId(in));
                    }
                    chunks.add(new LocatedChunk(handle, version, replicas));
                }
                return new GetChunkLocationsResponse(chunks);
            }
            case CREATE_FILE_REQUEST -> {
                return new CreateFileRequest(readString(in));
            }
            case CREATE_FILE_RESPONSE -> {
                return new CreateFileResponse(in.readBoolean());
            }
            case DELETE_FILE_REQUEST -> {
                return new DeleteFileRequest(readString(in));
            }
            case DELETE_FILE_RESPONSE -> {
                return new DeleteFileResponse(in.readBoolean());
            }
            case MKDIR_REQUEST -> {
                return new MkdirRequest(readString(in));
            }
            case MKDIR_RESPONSE -> {
                return new MkdirResponse(in.readBoolean());
            }
            case STAT_REQUEST -> {
                return new StatRequest(readString(in));
            }
            case STAT_RESPONSE -> {
                String path = readString(in);
                boolean isDirectory = in.readBoolean();
                long sizeBytes = in.readLong();
                long ctime = in.readLong();
                long mtime = in.readLong();
                return new StatResponse(path, isDirectory, sizeBytes, ctime, mtime);
            }
            case ERROR_RESPONSE -> {
                return new ErrorResponse(readString(in));
            }
            case READ_CHUNK_REQUEST -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                long offset = in.readLong();
                int size = in.readInt();
                return new ReadChunkRequest(handle, offset, size);
            }
            case READ_CHUNK_RESPONSE -> {
                boolean success = in.readBoolean();
                Bytes data = readBytes(in);
                String errorMessage = readString(in);
                return new ReadChunkResponse(success, data, errorMessage.isEmpty() ? null : errorMessage);
            }
            case PUSH_BYTES_REQUEST -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                Bytes data = readBytes(in);
                return new PushBytesRequest(handle, data);
            }
            case PUSH_BYTES_RESPONSE -> {
                boolean success = in.readBoolean();
                String errorMessage = readString(in);
                return new PushBytesResponse(success, errorMessage.isEmpty() ? null : errorMessage);
            }
            case COMMIT_WRITE_REQUEST -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                long offset = in.readLong();
                int size = in.readInt();
                int secCount = in.readByte() & 0xFF;
                List<ChunkserverId> secondaries = new java.util.ArrayList<>(secCount);
                for (int i = 0; i < secCount; i++) {
                    secondaries.add(readChunkserverId(in));
                }
                return new CommitWriteRequest(handle, offset, size, secondaries);
            }
            case COMMIT_RECORD_APPEND_REQUEST -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                int secCount = in.readByte() & 0xFF;
                List<ChunkserverId> secondaries = new java.util.ArrayList<>(secCount);
                for (int i = 0; i < secCount; i++) {
                    secondaries.add(readChunkserverId(in));
                }
                return new CommitRecordAppendRequest(handle, secondaries);
            }
            case COMMIT_RESPONSE -> {
                boolean success = in.readBoolean();
                long offset = in.readLong();
                String errorMessage = readString(in);
                return new CommitResponse(success, offset, errorMessage.isEmpty() ? null : errorMessage);
            }
            case REPLICATE_BYTES -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                Bytes data = readBytes(in);
                int chainCount = in.readByte() & 0xFF;
                List<ChunkserverId> remainingChain = new java.util.ArrayList<>(chainCount);
                for (int i = 0; i < chainCount; i++) {
                    remainingChain.add(readChunkserverId(in));
                }
                return new ReplicateBytes(handle, data, remainingChain);
            }
            case APPLY_MUTATION -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                long serial = in.readLong();
                long offset = in.readLong();
                int size = in.readInt();
                return new ApplyMutation(handle, serial, offset, size);
            }
            case APPLY_ACK -> {
                ChunkHandle handle = new ChunkHandle(in.readLong());
                long serial = in.readLong();
                boolean success = in.readBoolean();
                String errorMessage = readString(in);
                return new ApplyAck(handle, serial, success, errorMessage.isEmpty() ? null : errorMessage);
            }
            case HEARTBEAT -> {
                ChunkserverId id = readChunkserverId(in);
                long diskFreeBytes = in.readLong();
                int numChunksHeld = in.readInt();
                boolean leaseRenewalRequested = in.readBoolean();
                
                int addedCount = in.readInt();
                List<ChunkHandle> recentlyAddedOrChangedChunks = new java.util.ArrayList<>(addedCount);
                for (int i = 0; i < addedCount; i++) {
                    recentlyAddedOrChangedChunks.add(new ChunkHandle(in.readLong()));
                }
                
                int sizesCount = in.readInt();
                java.util.Map<ChunkHandle, Long> chunkSizes = new java.util.HashMap<>(sizesCount);
                for (int i = 0; i < sizesCount; i++) {
                    chunkSizes.put(new ChunkHandle(in.readLong()), in.readLong());
                }
                
                int staleCount = in.readInt();
                List<ChunkHandle> staleReplicaReports = new java.util.ArrayList<>(staleCount);
                for (int i = 0; i < staleCount; i++) {
                    staleReplicaReports.add(new ChunkHandle(in.readLong()));
                }
                return new Heartbeat(id, diskFreeBytes, numChunksHeld, recentlyAddedOrChangedChunks, chunkSizes, leaseRenewalRequested, staleReplicaReports);
            }
            case REPLICA_STATE_REPORT -> {
                ChunkserverId id = readChunkserverId(in);
                ChunkHandle handle = new ChunkHandle(in.readLong());
                ChunkVersion version = new ChunkVersion(in.readInt());
                long sizeBytes = in.readLong();
                return new ReplicaStateReport(id, handle, version, sizeBytes);
            }
            case GRANT_LEASE -> {
                ChunkHandle chunk = new ChunkHandle(in.readLong());
                ChunkserverId primary = readChunkserverId(in);
                long grantedAt = in.readLong();
                long expiresAt = in.readLong();
                return new GrantLease(chunk, primary, grantedAt, expiresAt);
            }
            case REVOKE_LEASE -> {
                return new RevokeLease(new ChunkHandle(in.readLong()));
            }
            case COPY_CHUNK -> {
                ChunkHandle chunk = new ChunkHandle(in.readLong());
                ChunkserverId source = readChunkserverId(in);
                return new CopyChunk(chunk, source);
            }
            case DELETE_CHUNK -> {
                return new DeleteChunk(new ChunkHandle(in.readLong()));
            }
            case HEARTBEAT_ACK -> {
                int deleteCount = in.readInt();
                List<ChunkHandle> chunksToDelete = new java.util.ArrayList<>(deleteCount);
                for (int i = 0; i < deleteCount; i++) {
                    chunksToDelete.add(new ChunkHandle(in.readLong()));
                }
                
                int copyCount = in.readInt();
                List<CopyCommand> chunksToCopy = new java.util.ArrayList<>(copyCount);
                for (int i = 0; i < copyCount; i++) {
                    chunksToCopy.add(new CopyCommand(new ChunkHandle(in.readLong()), readChunkserverId(in)));
                }
                return new HeartbeatAck(chunksToDelete, chunksToCopy);
            }
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeShort(0);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 65535) {
            throw new IllegalArgumentException("String too long: " + bytes.length);
        }
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readUnsignedShort();
        if (len == 0) {
            return "";
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeBytes(DataOutputStream out, Bytes b) throws IOException {
        if (b == null) {
            out.writeInt(0);
            return;
        }
        byte[] data = b.toByteArray();
        out.writeInt(data.length);
        out.write(data);
    }

    private static Bytes readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            throw new IOException("Negative byte array length: " + len);
        }
        if (len == 0) {
            return new Bytes(new byte[0]);
        }
        byte[] data = new byte[len];
        in.readFully(data);
        return new Bytes(data);
    }

    private static void writeChunkserverId(DataOutputStream out, ChunkserverId id) throws IOException {
        if (id == null) {
            out.write(new byte[4]);
            out.writeShort(0);
            return;
        }
        String host = id.host();
        int port = id.port();
        byte[] ipBytes;
        if (host.equals("localhost") || host.equals("127.0.0.1")) {
            ipBytes = new byte[]{127, 0, 0, 1};
        } else {
            InetAddress addr = InetAddress.getByName(host);
            ipBytes = addr.getAddress();
            if (ipBytes.length != 4) {
                ipBytes = new byte[]{127, 0, 0, 1};
            }
        }
        out.write(ipBytes);
        out.writeShort(port);
    }

    private static ChunkserverId readChunkserverId(DataInputStream in) throws IOException {
        byte[] ipBytes = new byte[4];
        in.readFully(ipBytes);
        int port = in.readUnsignedShort();
        InetAddress addr = InetAddress.getByAddress(ipBytes);
        return ChunkserverId.of(addr.getHostAddress(), port);
    }
}
