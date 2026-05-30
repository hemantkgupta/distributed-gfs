package gfs.common;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WireCodecTest {

    private void verifyRoundtrip(int reqId, MessageType type, Object payload) throws Exception {
        WireMessage original = new WireMessage(reqId, type, payload);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WireCodec.encode(original, baos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        WireMessage decoded = WireCodec.decode(bais);

        assertThat(decoded.reqId()).isEqualTo(original.reqId());
        assertThat(decoded.type()).isEqualTo(original.type());
        assertThat(decoded.payload()).isEqualTo(original.payload());
    }

    @Test
    void testGetChunkLocationsRequest() throws Exception {
        verifyRoundtrip(1, MessageType.GET_CHUNK_LOCATIONS_REQUEST,
                new GetChunkLocationsRequest("/foo/bar", 1000000L, 50000L));
    }

    @Test
    void testGetChunkLocationsResponse() throws Exception {
        LocatedChunk chunk1 = new LocatedChunk(
                ChunkHandle.of(12345L),
                new ChunkVersion(2),
                List.of(ChunkserverId.of("127.0.0.1", 5001), ChunkserverId.of("127.0.0.1", 5002))
        );
        LocatedChunk chunk2 = new LocatedChunk(
                ChunkHandle.of(67890L),
                new ChunkVersion(0),
                List.of()
        );
        verifyRoundtrip(2, MessageType.GET_CHUNK_LOCATIONS_RESPONSE,
                new GetChunkLocationsResponse(List.of(chunk1, chunk2)));

        // Empty response
        verifyRoundtrip(3, MessageType.GET_CHUNK_LOCATIONS_RESPONSE,
                new GetChunkLocationsResponse(List.of()));
    }

    @Test
    void testCreateFileRequestAndResponse() throws Exception {
        verifyRoundtrip(4, MessageType.CREATE_FILE_REQUEST, new CreateFileRequest("/a/b/c"));
        verifyRoundtrip(5, MessageType.CREATE_FILE_RESPONSE, new CreateFileResponse(true));
        verifyRoundtrip(6, MessageType.CREATE_FILE_RESPONSE, new CreateFileResponse(false));
    }

    @Test
    void testDeleteFileRequestAndResponse() throws Exception {
        verifyRoundtrip(7, MessageType.DELETE_FILE_REQUEST, new DeleteFileRequest("/a/b/c"));
        verifyRoundtrip(8, MessageType.DELETE_FILE_RESPONSE, new DeleteFileResponse(true));
        verifyRoundtrip(9, MessageType.DELETE_FILE_RESPONSE, new DeleteFileResponse(false));
    }

    @Test
    void testMkdirRequestAndResponse() throws Exception {
        verifyRoundtrip(10, MessageType.MKDIR_REQUEST, new MkdirRequest("/dir"));
        verifyRoundtrip(11, MessageType.MKDIR_RESPONSE, new MkdirResponse(true));
        verifyRoundtrip(12, MessageType.MKDIR_RESPONSE, new MkdirResponse(false));
    }

    @Test
    void testStatRequestAndResponse() throws Exception {
        verifyRoundtrip(13, MessageType.STAT_REQUEST, new StatRequest("/file"));
        verifyRoundtrip(14, MessageType.STAT_RESPONSE,
                new StatResponse("/file", false, 450000L, 1621234567890L, 1629876543210L));
        verifyRoundtrip(15, MessageType.STAT_RESPONSE,
                new StatResponse("/dir", true, 0L, 1621234567890L, 1629876543210L));
    }

    @Test
    void testErrorResponse() throws Exception {
        verifyRoundtrip(16, MessageType.ERROR_RESPONSE, new ErrorResponse("An internal error occurred"));
        verifyRoundtrip(17, MessageType.ERROR_RESPONSE, new ErrorResponse(""));
    }

    @Test
    void testReadChunkRequestAndResponse() throws Exception {
        verifyRoundtrip(18, MessageType.READ_CHUNK_REQUEST, new ReadChunkRequest(ChunkHandle.of(99L), 1024L, 65536));
        
        verifyRoundtrip(19, MessageType.READ_CHUNK_RESPONSE,
                new ReadChunkResponse(true, new Bytes(new byte[]{5, 6, 7, 8}), null));
        verifyRoundtrip(20, MessageType.READ_CHUNK_RESPONSE,
                new ReadChunkResponse(false, new Bytes(new byte[0]), "Checksum mismatch"));
    }

    @Test
    void testPushBytesRequestAndResponse() throws Exception {
        verifyRoundtrip(21, MessageType.PUSH_BYTES_REQUEST,
                new PushBytesRequest(ChunkHandle.of(42L), new Bytes(new byte[]{1, 2, 3})));
        verifyRoundtrip(22, MessageType.PUSH_BYTES_RESPONSE, new PushBytesResponse(true, null));
        verifyRoundtrip(23, MessageType.PUSH_BYTES_RESPONSE, new PushBytesResponse(false, "Buffer full"));
    }

    @Test
    void testCommitWriteRequest() throws Exception {
        verifyRoundtrip(24, MessageType.COMMIT_WRITE_REQUEST,
                new CommitWriteRequest(
                        ChunkHandle.of(77L),
                        1024L,
                        2048,
                        List.of(ChunkserverId.of("127.0.0.1", 6001), ChunkserverId.of("127.0.0.1", 6002))
                ));
        verifyRoundtrip(25, MessageType.COMMIT_WRITE_REQUEST,
                new CommitWriteRequest(ChunkHandle.of(77L), 1024L, 2048, List.of()));
    }

    @Test
    void testCommitRecordAppendRequest() throws Exception {
        verifyRoundtrip(26, MessageType.COMMIT_RECORD_APPEND_REQUEST,
                new CommitRecordAppendRequest(
                        ChunkHandle.of(77L),
                        List.of(ChunkserverId.of("127.0.0.1", 6001))
                ));
    }

    @Test
    void testCommitResponse() throws Exception {
        verifyRoundtrip(27, MessageType.COMMIT_RESPONSE, new CommitResponse(true, 4096L, null));
        verifyRoundtrip(28, MessageType.COMMIT_RESPONSE, new CommitResponse(false, -1L, "Lease expired"));
    }

    @Test
    void testReplicateBytes() throws Exception {
        verifyRoundtrip(29, MessageType.REPLICATE_BYTES,
                new ReplicateBytes(
                        ChunkHandle.of(88L),
                        new Bytes(new byte[]{10, 11}),
                        List.of(ChunkserverId.of("127.0.0.1", 6002))
                ));
    }

    @Test
    void testApplyMutationAndAck() throws Exception {
        verifyRoundtrip(30, MessageType.APPLY_MUTATION, new ApplyMutation(ChunkHandle.of(33L), 555L, 2048L, 1024));
        verifyRoundtrip(31, MessageType.APPLY_ACK, new ApplyAck(ChunkHandle.of(33L), 555L, true, null));
        verifyRoundtrip(32, MessageType.APPLY_ACK, new ApplyAck(ChunkHandle.of(33L), 555L, false, "Disk I/O error"));
    }

    @Test
    void testHeartbeat() throws Exception {
        verifyRoundtrip(33, MessageType.HEARTBEAT,
                new Heartbeat(
                        ChunkserverId.of("127.0.0.1", 8000),
                        1000000000L,
                        5,
                        List.of(ChunkHandle.of(1L), ChunkHandle.of(2L)),
                        Map.of(ChunkHandle.of(1L), 2000L, ChunkHandle.of(2L), 4000L),
                        true,
                        List.of(ChunkHandle.of(3L))
                ));
    }

    @Test
    void testReplicaStateReport() throws Exception {
        verifyRoundtrip(34, MessageType.REPLICA_STATE_REPORT,
                new ReplicaStateReport(ChunkserverId.of("127.0.0.1", 8000), ChunkHandle.of(10L), new ChunkVersion(5), 4096000L));
    }

    @Test
    void testGrantLease() throws Exception {
        verifyRoundtrip(35, MessageType.GRANT_LEASE,
                new GrantLease(ChunkHandle.of(10L), ChunkserverId.of("127.0.0.1", 8000), 1620000000000L, 1620000060000L));
    }

    @Test
    void testRevokeLease() throws Exception {
        verifyRoundtrip(36, MessageType.REVOKE_LEASE, new RevokeLease(ChunkHandle.of(10L)));
    }

    @Test
    void testCopyChunk() throws Exception {
        verifyRoundtrip(37, MessageType.COPY_CHUNK, new CopyChunk(ChunkHandle.of(15L), ChunkserverId.of("127.0.0.1", 8001)));
    }

    @Test
    void testDeleteChunk() throws Exception {
        verifyRoundtrip(38, MessageType.DELETE_CHUNK, new DeleteChunk(ChunkHandle.of(15L)));
    }

    @Test
    void testHeartbeatAck() throws Exception {
        verifyRoundtrip(39, MessageType.HEARTBEAT_ACK,
                new HeartbeatAck(
                        List.of(ChunkHandle.of(99L), ChunkHandle.of(100L)),
                        List.of(new CopyCommand(ChunkHandle.of(101L), ChunkserverId.of("127.0.0.1", 9000)))
                ));

        verifyRoundtrip(40, MessageType.HEARTBEAT_ACK, new HeartbeatAck(List.of(), List.of()));
    }
}
