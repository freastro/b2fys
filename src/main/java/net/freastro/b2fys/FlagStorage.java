package net.freastro.b2fys;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FlagStorage {

    private static final Logger log = LogManager.getLogger(FlagStorage.class);

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

    FlagStorage() {
    }

    FlagStorage(Config config) {
        mountOptions = new HashMap<>(config.mountOptions);
        mountPoint = config.mountPoint;

        cache = config.cache;
        dirMode = config.dirMode;
        fileMode = config.fileMode;
        uid = config.uid;
        gid = config.gid;

        endpoint = config.endpoint;
        region = config.region;
        regionSet = config.regionSet;
        storageClass = config.storageClass;
        profile = config.profile;
        useContentType = config.useContentType;
        useSSE = config.useSSE;
        useKMS = config.useKMS;
        kmsKeyId = config.kmsKeyId;
        acl = config.acl;

        cheap = config.cheap;
        explicitDir = config.explicitDir;
        statCacheTtl = config.statCacheTtl;
        TypeCacheTtl = config.TypeCacheTtl;

        debugFuse = config.debugFuse;
        debugS3 = config.debugS3;
        foreground = config.foreground;
    }

    static String[] massageMountFlags(String[] args) {
        List<String> ret = new ArrayList<>();
        if (args.length == 5 && args[3].equals("-o")) {
            // looks like it's coming from fstab!
            String mountOptions = "";
            ret.add(args[0]);

            for (String p : args[4].split(",")) {
                if (p.startsWith("-")) {
                    ret.add(p);
                } else {
                    mountOptions += p;
                    mountOptions += ",";
                }
            }

            if (mountOptions.length() != 0) {
                // remote trailing ,
                mountOptions = mountOptions.substring(0, mountOptions.length() - 1);
                ret.add("-o");
                ret.add(mountOptions);
            }

            ret.add(args[1]);
            ret.add(args[2]);
        } else {
            return args;
        }
        return ret.toArray(new String[ret.size()]);
    }

    static void parseOptions(Map<String, String> m, String s) {
        // NOTE(jacobsa): The man pages don't define how escaping works, and as far
        // as I can tell there is no way to properly escape or quote a comma in the
        // options list for an fstab entry. So put our fingers in our ears and hope
        // that nobody needs a comma.
        for (String p : s.split(",")) {
            String name;
            String value = null;

            // Split on the first equals sign.
            int equalsIndex = p.indexOf('=');
            if (equalsIndex != -1) {
                name = p.substring(0, equalsIndex);
                value = p.substring(equalsIndex + 1);
            } else {
                name = p;
            }

            m.put(name, value);
        }

        return;
    }

    static FlagStorage populateFlags(App c) {
        FlagStorage flags = new FlagStorage();
        flags.mountOptions = new HashMap<>();
        // File system
        flags.dirMode = c.dirMode;
        flags.fileMode = c.fileMode;
        flags.uid = c.uid;
        flags.gid = c.gid;
        // Tuning,
        flags.cheap = c.cheap;
        flags.explicitDir = c.noImplicitDir;
        flags.statCacheTtl = Duration.of(c.statCacheTtl, ChronoUnit.SECONDS);
        flags.TypeCacheTtl = Duration.of(c.typeCacheTtl, ChronoUnit.SECONDS);
        // S3
        flags.endpoint = c.endpoint;
        flags.region = c.region;
        flags.regionSet = (c.region != null && !c.region.isEmpty());
        flags.storageClass = c.storageClass;
        flags.profile = c.profile;
        flags.useContentType = c.useContentType;
        flags.useSSE = c.sse;
        flags.useKMS = (c.sseKms != null && !c.sseKms.isEmpty());
        flags.kmsKeyId = c.sseKms;
        flags.acl = c.acl;
        // Debugging
        flags.debugFuse = c.debugFuse;
        flags.debugS3 = c.debugS3;
        flags.foreground = c.f;

        // Handle the repated "-o" flag.
        for (String o : c.o) {
            parseOptions(flags.mountOptions, o);
        }

        flags.mountPointArg = c.args.get(1);
        flags.mountPoint = flags.mountPointArg;

        if (c.cache != null && !c.cache.isEmpty()) {
            // TODO(ghart): cache
        }

        // KMS implies SSE
        if (flags.useKMS) {
            flags.useSSE = true;
        }

        return flags;
    }

    void cleanup() {
        if (!mountPointCreated.equals("") && !mountPointCreated.equals(mountPointArg)) {
            boolean err = new File(mountPointCreated).delete();
            if (!err) {
                log.error("rmdir %s = %s", mountPointCreated, err);
            }
        }
    }
}
