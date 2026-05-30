package gfs.common;

public enum MessageType {
    // Client → Master
    GET_CHUNK_LOCATIONS_REQUEST(0x10),
    CREATE_FILE_REQUEST(0x11),
    DELETE_FILE_REQUEST(0x12),
    MKDIR_REQUEST(0x13),
    STAT_REQUEST(0x14),

    // Master → Client (responses)
    GET_CHUNK_LOCATIONS_RESPONSE(0x20),
    CREATE_FILE_RESPONSE(0x21),
    DELETE_FILE_RESPONSE(0x22),
    MKDIR_RESPONSE(0x23),
    STAT_RESPONSE(0x24),
    ERROR_RESPONSE(0x2F),

    // Client → Chunkserver
    READ_CHUNK_REQUEST(0x30),
    PUSH_BYTES_REQUEST(0x31),
    COMMIT_WRITE_REQUEST(0x32),
    COMMIT_RECORD_APPEND_REQUEST(0x33),

    // Chunkserver → Client (responses)
    READ_CHUNK_RESPONSE(0x40),
    PUSH_BYTES_RESPONSE(0x41),
    COMMIT_RESPONSE(0x42),

    // Chunkserver → Chunkserver (replication chain)
    REPLICATE_BYTES(0x50),
    APPLY_MUTATION(0x51),
    APPLY_ACK(0x52),

    // Chunkserver → Master
    HEARTBEAT(0x60),
    REPLICA_STATE_REPORT(0x61),

    // Master → Chunkserver
    GRANT_LEASE(0x70),
    REVOKE_LEASE(0x71),
    COPY_CHUNK(0x72),         // re-replication command
    DELETE_CHUNK(0x73),       // GC command
    HEARTBEAT_ACK(0x74);

    private final byte code;

    MessageType(int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }

    public static MessageType fromCode(byte code) {
        for (MessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown message type code: " + String.format("0x%02X", code));
    }
}
