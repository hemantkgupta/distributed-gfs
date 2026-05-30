package gfs.common;

import java.time.Instant;

public record LeaseToken(
    ChunkHandle chunk,
    ChunkserverId primary,
    Instant grantedAt,
    Instant expiresAt
) {
    public boolean isValid(Instant now) {
        return now.isBefore(expiresAt);
    }
}
