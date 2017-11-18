package net.freastro.b2fys;

import java.time.Duration;
import java.util.Map;

class FlagStorage {

    // File system
    Map<String, String> mountOptions;
    String mountPoint;
    String mountPointArg;
    String mountPointCreated;

    String[] cache;
    int dirMode;
    int fileMode;
    int uid;
    int gid;

    // S3
    String endpoint;
    String region;
    boolean regionSet;
    String storageClass;
    String profile;
    boolean useContentType;
    boolean useSSE;
    boolean useKMS;
    String kmsKeyId;
    String acl;

    // Tuning
    boolean cheap;
    boolean explicitDir;
    Duration statCacheTtl;
    Duration TypeCacheTtl;

    // Debugging
    boolean debugFuse;
    boolean debugS3;
    boolean foreground;
}
