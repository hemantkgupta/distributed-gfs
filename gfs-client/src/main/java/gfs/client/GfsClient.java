package gfs.client;

import gfs.common.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GfsClient implements AutoCloseable {
    private final ChunkserverId masterId;
    private final String clientHost;
    private final int clientPort;
    private final MetadataCache cache;
    private final Clock clock;
    private final AtomicInteger reqIdGenerator = new AtomicInteger(1);
    private final ConcurrentHashMap<Long, Object> chunkLocks = new ConcurrentHashMap<>();

    public GfsClient(ChunkserverId masterId, String clientHost, int clientPort, Clock clock) {
        this.masterId = masterId;
        this.clientHost = clientHost;
        this.clientPort = clientPort;
        this.clock = clock;
        this.cache = new MetadataCache(clock);
    }

    public GfsClient(ChunkserverId masterId, String clientHost, int clientPort) {
        this(masterId, clientHost, clientPort, Clock.systemDefaultZone());
    }

    public GfsClient(ChunkserverId masterId) {
        this(masterId, "127.0.0.1", 0);
    }

    private int nextReqId() {
        return reqIdGenerator.getAndIncrement();
    }

    private Object getLockForChunk(ChunkHandle handle) {
        return chunkLocks.computeIfAbsent(handle.id(), id -> new Object());
    }

    private WireMessage sendRpc(ChunkserverId target, MessageType type, Object payload) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(target.host(), target.port()), 3000);
            socket.setSoTimeout(3000);
            int reqId = nextReqId();
            WireMessage msg = new WireMessage(reqId, type, payload);
            WireCodec.encode(msg, socket.getOutputStream());
            return WireCodec.decode(socket.getInputStream());
        }
    }

    public synchronized List<LocatedChunk> getOrFetchLocations(String path, long offset, long size) throws IOException {
        int startIdx = OffsetMath.chunkIndex(offset);
        int endIdx = OffsetMath.chunkIndex(offset + size - 1);

        Optional<List<LocatedChunk>> cached = cache.get(path, startIdx, endIdx);
        if (cached.isPresent()) {
            return cached.get();
        }

        WireMessage resp = sendRpc(masterId, MessageType.GET_CHUNK_LOCATIONS_REQUEST,
                new GetChunkLocationsRequest(path, offset, size));
        if (resp.payload() instanceof GetChunkLocationsResponse gcr) {
            List<LocatedChunk> locations = gcr.chunks();
            if (locations.isEmpty()) {
                throw new IOException("No chunk locations returned from master for path " + path);
            }
            cache.put(path, startIdx, locations);
            return locations;
        } else if (resp.payload() instanceof ErrorResponse er) {
            throw new IOException("Failed to get locations from master: " + er.errorMessage());
        }
        throw new IOException("Invalid response from master when fetching chunk locations");
    }

    public boolean create(String path) throws IOException {
        WireMessage resp = sendRpc(masterId, MessageType.CREATE_FILE_REQUEST, new CreateFileRequest(path));
        if (resp.payload() instanceof CreateFileResponse r) {
            return r.success();
        }
        return false;
    }

    public boolean mkdir(String path) throws IOException {
        WireMessage resp = sendRpc(masterId, MessageType.MKDIR_REQUEST, new MkdirRequest(path));
        if (resp.payload() instanceof MkdirResponse r) {
            return r.success();
        }
        return false;
    }

    public boolean delete(String path) throws IOException {
        WireMessage resp = sendRpc(masterId, MessageType.DELETE_FILE_REQUEST, new DeleteFileRequest(path));
        if (resp.payload() instanceof DeleteFileResponse r) {
            cache.invalidate(path);
            return r.success();
        }
        return false;
    }

    public StatResponse stat(String path) throws IOException {
        WireMessage resp = sendRpc(masterId, MessageType.STAT_REQUEST, new StatRequest(path));
        if (resp.payload() instanceof StatResponse sr) {
            return sr;
        }
        return null;
    }

    public byte[] read(String path, long offset, int size) throws IOException {
        if (size <= 0) {
            return new byte[0];
        }

        List<Integer> chunkSizes = OffsetMath.bytesNeededAcrossChunks(offset, size);
        byte[] result = new byte[size];
        int resultOffset = 0;

        int startIdx = OffsetMath.chunkIndex(offset);

        for (int i = 0; i < chunkSizes.size(); i++) {
            int chunkIdx = startIdx + i;
            long chunkOffset = (i == 0) ? OffsetMath.offsetInChunk(offset) : 0;
            int readSize = chunkSizes.get(i);

            long targetOffset = (long) chunkIdx * OffsetMath.CHUNK_SIZE;
            List<LocatedChunk> locations = getOrFetchLocations(path, targetOffset, 1);
            if (locations.isEmpty()) {
                throw new IOException("Failed to locate chunk " + chunkIdx + " for file " + path);
            }
            LocatedChunk locatedChunk = locations.get(0);

            byte[] chunkBytes = null;
            List<ChunkserverId> orderedReplicas = ChainReplicationDriver.sortServers(clientHost, clientPort, locatedChunk.replicas());
            IOException lastException = null;

            for (ChunkserverId replica : orderedReplicas) {
                try {
                    WireMessage resp = sendRpc(replica, MessageType.READ_CHUNK_REQUEST,
                            new ReadChunkRequest(locatedChunk.handle(), chunkOffset, readSize));
                    if (resp.payload() instanceof ReadChunkResponse rcr) {
                        if (rcr.success()) {
                            chunkBytes = rcr.data().toByteArray();
                            break;
                        } else {
                            lastException = new IOException("Replica " + replica + " returned error: " + rcr.errorMessage());
                        }
                    } else if (resp.payload() instanceof ErrorResponse er) {
                        lastException = new IOException("Replica " + replica + " returned ErrorResponse: " + er.errorMessage());
                    } else {
                        lastException = new IOException("Replica " + replica + " returned unexpected payload");
                    }
                } catch (IOException e) {
                    lastException = e;
                }
            }

            if (chunkBytes == null) {
                cache.invalidate(locatedChunk.handle());
                throw new IOException("Failed to read chunk " + locatedChunk.handle() + " from all replicas. Last error: " +
                        (lastException != null ? lastException.getMessage() : "unknown"), lastException);
            }

            System.arraycopy(chunkBytes, 0, result, resultOffset, readSize);
            resultOffset += readSize;
        }

        return result;
    }

    public void write(String path, long offset, byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        List<Integer> chunkSizes = OffsetMath.bytesNeededAcrossChunks(offset, bytes.length);
        int bytesWritten = 0;
        int startIdx = OffsetMath.chunkIndex(offset);

        for (int i = 0; i < chunkSizes.size(); i++) {
            int chunkIdx = startIdx + i;
            long chunkOffset = (i == 0) ? OffsetMath.offsetInChunk(offset) : 0;
            int writeSize = chunkSizes.get(i);
            byte[] chunkData = Arrays.copyOfRange(bytes, bytesWritten, bytesWritten + writeSize);

            long targetOffset = (long) chunkIdx * OffsetMath.CHUNK_SIZE;

            int retryCount = 0;
            boolean success = false;
            IOException lastEx = null;

            while (retryCount < 3 && !success) {
                try {
                    List<LocatedChunk> locations = getOrFetchLocations(path, targetOffset, 1);
                    if (locations.isEmpty()) {
                        throw new IOException("Failed to locate chunk " + chunkIdx + " for file " + path);
                    }
                    LocatedChunk locatedChunk = locations.get(0);

                    synchronized (getLockForChunk(locatedChunk.handle())) {
                        ChunkserverId primary = locatedChunk.replicas().get(0);
                        List<ChunkserverId> secondaries = locatedChunk.replicas().subList(1, locatedChunk.replicas().size());

                        List<ChunkserverId> pipeline = ChainReplicationDriver.sortServers(clientHost, clientPort, locatedChunk.replicas());

                        WireMessage pushResp = sendRpc(pipeline.get(0), MessageType.REPLICATE_BYTES,
                                new ReplicateBytes(locatedChunk.handle(), new Bytes(chunkData), pipeline.subList(1, pipeline.size())));

                        if (!(pushResp.payload() instanceof PushBytesResponse pbr) || !pbr.success()) {
                            String err = (pushResp.payload() instanceof PushBytesResponse pbr2) ? pbr2.errorMessage() : "Unknown push bytes response";
                            throw new IOException("Daisy-chain PushBytes failed: " + err);
                        }

                        WireMessage commitResp = sendRpc(primary, MessageType.COMMIT_WRITE_REQUEST,
                                new CommitWriteRequest(locatedChunk.handle(), chunkOffset, writeSize, secondaries));

                        if (commitResp.payload() instanceof CommitResponse cr) {
                            if (cr.success()) {
                                success = true;
                            } else {
                                throw new IOException("CommitWrite failed on primary " + primary + ": " + cr.errorMessage());
                            }
                        } else if (commitResp.payload() instanceof ErrorResponse er) {
                            throw new IOException("CommitWrite ErrorResponse from primary " + primary + ": " + er.errorMessage());
                        } else {
                            throw new IOException("Invalid CommitWrite response from primary " + primary);
                        }
                    }

                } catch (IOException e) {
                    lastEx = e;
                    retryCount++;
                    cache.invalidate(path);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            if (!success) {
                throw new IOException("Write failed for chunk " + chunkIdx + " after 3 retries", lastEx);
            }

            bytesWritten += writeSize;
        }
    }

    public long recordAppend(String path, byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Cannot append empty record");
        }
        if (bytes.length > 16 * 1024 * 1024) {
            throw new IllegalArgumentException("Record size exceeds maximum allowed append size (16 MB)");
        }

        int retryCount = 0;
        while (retryCount < 10) {
            StatResponse statVal = stat(path);
            if (statVal == null) {
                throw new IOException("File not found: " + path);
            }

            long fileLength = statVal.sizeBytes();
            long lastChunkOffset = fileLength > 0 ? fileLength - 1 : 0;
            int chunkIdx = OffsetMath.chunkIndex(lastChunkOffset);

            List<LocatedChunk> locations;
            try {
                locations = getOrFetchLocations(path, lastChunkOffset, 1);
            } catch (IOException e) {
                retryCount++;
                cache.invalidate(path);
                try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                continue;
            }

            if (locations.isEmpty()) {
                throw new IOException("Could not locate last chunk for file " + path);
            }
            LocatedChunk locatedChunk = locations.get(0);

            synchronized (getLockForChunk(locatedChunk.handle())) {
                ChunkserverId primary = locatedChunk.replicas().get(0);
                List<ChunkserverId> secondaries = locatedChunk.replicas().subList(1, locatedChunk.replicas().size());

                List<ChunkserverId> pipeline = ChainReplicationDriver.sortServers(clientHost, clientPort, locatedChunk.replicas());

                try {
                    WireMessage pushResp = sendRpc(pipeline.get(0), MessageType.REPLICATE_BYTES,
                            new ReplicateBytes(locatedChunk.handle(), new Bytes(bytes), pipeline.subList(1, pipeline.size())));

                    if (!(pushResp.payload() instanceof PushBytesResponse pbr) || !pbr.success()) {
                        retryCount++;
                        cache.invalidate(locatedChunk.handle());
                        try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                        continue;
                    }
                } catch (IOException e) {
                    retryCount++;
                    cache.invalidate(locatedChunk.handle());
                    try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    continue;
                }

                try {
                    WireMessage commitResp = sendRpc(primary, MessageType.COMMIT_RECORD_APPEND_REQUEST,
                            new CommitRecordAppendRequest(locatedChunk.handle(), secondaries));

                    if (commitResp.payload() instanceof CommitResponse cr) {
                        if (cr.success()) {
                            return (long) chunkIdx * OffsetMath.CHUNK_SIZE + cr.offset();
                        } else if ("RETRY_ON_NEXT_CHUNK".equals(cr.errorMessage())) {
                            long nextChunkOffset = (long) (chunkIdx + 1) * OffsetMath.CHUNK_SIZE;
                            cache.invalidate(path);
                            getOrFetchLocations(path, nextChunkOffset, 1);
                        } else {
                            retryCount++;
                            cache.invalidate(locatedChunk.handle());
                            try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                        }
                    } else if (commitResp.payload() instanceof ErrorResponse er) {
                        retryCount++;
                        cache.invalidate(locatedChunk.handle());
                        try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    } else {
                        retryCount++;
                        cache.invalidate(locatedChunk.handle());
                        try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    }
                } catch (IOException e) {
                    retryCount++;
                    cache.invalidate(locatedChunk.handle());
                    try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }

        throw new IOException("Record append failed after 10 attempts");
    }

    public MetadataCache getCache() {
        return cache;
    }

    @Override
    public void close() {
        cache.clear();
        chunkLocks.clear();
    }
}
