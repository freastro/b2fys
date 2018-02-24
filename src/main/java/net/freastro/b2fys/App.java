package net.freastro.b2fys;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import co.paralleluniverse.fuse.Fuse;
import co.paralleluniverse.fuse.FuseFilesystem;

public class App {

    private static final Logger log = LogManager.getLogger(App.class);

    @Parameter
    List<String> args = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, description = "Print this help and exit successfully.")
    boolean help;

    // -----------
    // File system
    // -----------

    @Parameter(names = "-o", description = "Additional system-specific mount options. Be careful!")
    List<String> o = new ArrayList<>();

    @Parameter(names = "--cache", description = "Directory to use for data cache. "
                                                + "Requires catfs and `-o allow_other'. "
                                                + "Can also pass in other catfs options "
                                                + "(ex: --cache \"--free:10%:$HOME/cache\" "
                                                + "(default: off)")
    String cache;

    @Parameter(names = "--dir-mode", description = "Permission bits for directories. (default: "
                                                   + "0755)")
    int dirMode = 0755;

    @Parameter(names = "--file-mode", description = "Permission bits for files. (default: 0644)")
    int fileMode = 0644;

    @Parameter(names = "--uid", description = "UID owner of all inodes.")
    int uid = myUser();

    @Parameter(names = "--gid", description = "GID owner of all inodes.")
    int gid = myGroup();

    // --
    // S3
    // --

    @Parameter(names = "--endpoint", description = "The non-AWS endpoint to connect to."
                                                   + " Possible values: http://127.0.0.1:8081/")
    String endpoint = "";

    @Parameter(names = "--region", description = "The region to connect to. Usually this is "
                                                 + "auto-detected."
                                                 + " Possible values: us-east-1, us-west-1, "
                                                 + "us-west-2, eu-west-1, "
                                                 + "eu-central-1, ap-southeast-1, ap-southeast-2,"
                                                 + " ap-northeast-1, "
                                                 + "sa-east-1, cn-north-1")
    String region;

    @Parameter(names = "--storage-class", description = "The type of storage to use when writing "
                                                        + "objects."
                                                        + " Possible values: REDUCED_REDUNDANCY, "
                                                        + "STANDARD, STANDARD_IA.")
    String storageClass;

    @Parameter(names = "--profile", description = "Use a named profile from $HOME/"
                                                  + ".aws/credentials instead of \"/default\"")
    String profile;

    @Parameter(names = "--use-content-type", description = "Set Content-Type according to file "
                                                           + "extension and /etc/mime.types "
                                                           + "(default: off")
    boolean useContentType;

    @Parameter(names = "--sse", description = "Enable basic server-side encryption at rest "
                                              + "(SSE-S3) in S3 for all writes (default: off)")
    boolean sse;

    @Parameter(names = "--sse-kms", description = "Enable KMS encryption (SSE-KMS) for all writes "
                                                  + "using this particular KMS `key-id`. Leave "
                                                  + "blank to Use the account's CMK - customer "
                                                  + "master key (default: off)")
    String sseKms = "";

    @Parameter(names = "--acl", description = "The canned ACL to applyto the object. Possible "
                                              + "values: private, public-read, public-read-write,"
                                              + " authenticated-read, aws-exec-read, "
                                              + "bucket-owner-read, bucket-owner-full-control "
                                              + "(default: off")
    String acl;

    // ------
    // Tuning
    // ------

    @Parameter(names = "--cheap", description = "Reduce S3 operation costs at the expense of some"
                                                + " performance (default: off)")
    boolean cheap;

    @Parameter(names = "--no-implicit-dir", description = "Assume all directory objects (\"dir\")"
                                                          + " exist (default: off)")
    boolean noImplicitDir;

    @Parameter(names = "--stat-cache-ttl", description = "How long to cache StatObject results "
                                                         + "and inode attributes")
    int statCacheTtl = 60;

    @Parameter(names = "--type-cache-ttl", description = "How long to cache name -> file/dir "
                                                         + "mappings in directory "
                                                         + "inodes.")
    int typeCacheTtl = 60;

    // ---------
    // Debugging
    // ---------

    @Parameter(names = "--debug_fuse", description = "Enable fuse-related debugging output.")
    boolean debugFuse;

    @Parameter(names = "--debug_s3", description = "Enable S3-related debugging output.")
    boolean debugS3;

    @Parameter(names = "-f", description = "Run goofys in foreground.")
    boolean f;

    public static void main(String[] args) throws Exception {
        FlagStorage flags;

        App app = new App();
        new JCommander(app).parse(args);

        args = FlagStorage.massageMountFlags(app.args.toArray(new String[0]));

        // We should get two arguments exactly. Otherwise error out.
        if (args.length != 2) {
            System.err.print("Error: b2fys takes exactly two arguments.\n\n");
//            cli.showAppHelp(c)
            System.exit(1);
        }

        // Populate and parse flags
        String bucketName = args[0];
        flags = FlagStorage.populateFlags(app);
        if (flags == null) {
//            cli.showAppHelp(c)
            System.err.println("invalid arguments");
            System.exit(1);
        }

        // Mount the file system
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            System.err.println("ERROR " + t.getName() + " exited due to: " + e.toString());
            e.printStackTrace();
        });
        FuseFilesystem fs = mount(bucketName, flags);

        log.info("File system has been successfully mounted.");
        // Let the user unmount with Ctrl-C
        // (SIGINT). But if cache is on, catfs will
        // receive the signal and we would detect that exiting
        registerSIGINTHandler(fs, flags);

        flags.cleanup();
        Thread.sleep(Long.MAX_VALUE);
    }

    // Mount the file system based on the supplied arguments, returning a
    // fuse.MountedFileSystem that can be joined to wait for unmounting.
    static FuseFilesystem mount(String bucketName, FlagStorage flags) {

        // XXX really silly copy here! in goofys.Mount we will copy it
        // back to FlagStorage. But I don't see a easier way to expose
        // Config in the api package
        Config config = new Config(flags);

        return B2FuseFilesystem.Mount(bucketName, config);
    }

    int myGroup() {
        try {
            return Integer.parseInt(new ProcessGobbler("id", "-g").getStdout());
        } catch (IOException e) {
            return 0;
        }
    }

    int myUser() {
        try {
            return Integer.parseInt(new ProcessGobbler("id", "-u").getStdout());
        } catch (IOException e) {
            return 0;
        }
    }

    static void registerSIGINTHandler(FuseFilesystem fs, FlagStorage flags) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (flags.cache.length == 0) {
                    log.info("Received SIGINT, attempting to unmount...");

                    boolean err = tryUnmount(flags.mountPoint);
                    if (!err) {
                        log.info("Failed to unmount in response to SIGINT: %s", err);
                    } else {
                        log.info("Successfully unmounted %s in response to SIGINT",
                                 flags.mountPoint);
                    }
                } else {
                    log.info("Received SIGINT");
                    // wait for catfs to die and cleanup
                }
            }
        });
    }

    static boolean tryUnmount(String mountPoint) {
        boolean err = true;
        for (int i = 0; i < 20; ++i) {
            try {
                Fuse.unmount(Paths.get(mountPoint));
                err = false;
                break;
            } catch (IOException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // ignored
                }
            }
        }
        return err;
    }
}
