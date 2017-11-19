package net.freastro.b2fys;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

class Config {

    // File system
    Map<String, String> mountOptions;
    String mountPoint;

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

    Config() {
    }

    Config(FlagStorage flags) {
        mountOptions = new HashMap<>(flags.mountOptions);
        mountPoint = flags.mountPoint;

        cache = flags.cache;
        dirMode = flags.dirMode;
        fileMode = flags.fileMode;
        uid = flags.uid;
        gid = flags.gid;

        endpoint = flags.endpoint;
        region = flags.region;
        regionSet = flags.regionSet;
        storageClass = flags.storageClass;
        profile = flags.profile;
        useContentType = flags.useContentType;
        useSSE = flags.useSSE;
        useKMS = flags.useKMS;
        kmsKeyId = flags.kmsKeyId;
        acl = flags.acl;

        cheap = flags.cheap;
        explicitDir = flags.explicitDir;
        statCacheTtl = flags.statCacheTtl;
        TypeCacheTtl = flags.TypeCacheTtl;

        debugFuse = flags.debugFuse;
        debugS3 = flags.debugS3;
        foreground = flags.foreground;
    }
}
