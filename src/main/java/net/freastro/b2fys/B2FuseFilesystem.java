package net.freastro.b2fys;

import com.backblaze.b2.client.B2ClientConfig;
import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;
import com.backblaze.b2.client.webApiHttpClient.B2StorageHttpClientBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.awt.Mutex;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import co.paralleluniverse.fuse.AbstractFuseFilesystem;
import co.paralleluniverse.fuse.DirectoryFiller;
import co.paralleluniverse.fuse.StructFuseFileInfo;
import co.paralleluniverse.fuse.StructStat;
import co.paralleluniverse.fuse.StructStatvfs;
import co.paralleluniverse.fuse.XattrFiller;
import co.paralleluniverse.fuse.XattrListFiller;
import jnr.constants.platform.Errno;

import static net.freastro.b2fys.fuseops.RootInodeID;
import static net.freastro.b2fys.syscall.DT_Directory;

public class B2FuseFilesystem extends AbstractFuseFilesystem {

    private static final Logger log = LogManager.getLogger(B2FuseFilesystem.class);

    String bucket;
    String prefix;

    FlagStorage flags;

    int umask;

    B2ClientConfig awsConfig;
    Object sess;
    B2StorageClient b2;
    boolean v2Signer;
    String seeType;
    InodeAttributes rootAttrs;

    BufferPool bufferPool;

    // A lock protecting the state of the file system struct itself (distinct
    // from per-inode locks). Make sure to see the notes on lock ordering above.
    Mutex mu;

    // The next inode ID to hand out. We assume that this will never overflow,
    // since even if we were handing out inode IDs at 4 GHz, it would still take
    // over a century to do so.
    //
    // GUARDED_BY(mu)
    long nextInodeID;

    // The collection of live inodes, keyed by inode ID. No ID less than
    // fuseops.RootInodeID is ever used.
    //
    // INVARIANT: For all keys k, fuseops.RootInodeID <= k < nextInodeID
    // INVARIANT: For all keys k, inodes[k].ID() == k
    // INVARIANT: inodes[fuseops.RootInodeID] is missing or of type inode.DirInode
    // INVARIANT: For all v, if IsDirName(v.Name()) then v is inode.DirInode
    //
    // GUARDED_BY(mu)
    Map<Long, Inode> inodes;
    Map<String, Long> paths = new HashMap<>();

    long nextHandleID;
    Map<Long, DirHandle> dirHandles;
    Map<Long, FileHandle> fileHandles;

    Ticket replicators;
    Ticket restorers;

    int forgotCnt;

    private B2FuseFilesystem() {
    }

    public static B2FuseFilesystem create(String bucket, B2ClientConfig b2Config,
                                          FlagStorage flags) {
        B2FuseFilesystem fs = new B2FuseFilesystem();
        fs.bucket = bucket;
        fs.flags = flags;
        fs.umask = 0122;

        int colon = bucket.indexOf(':');
        if (colon != -1) {
            fs.prefix = bucket.substring(colon + 1);
            if (fs.prefix.startsWith("/")) {
                fs.prefix = fs.prefix.substring(1);
            }
            if (fs.prefix.endsWith("/")) {
                fs.prefix = fs.prefix.substring(0, fs.prefix.length() - 1);
            }
            fs.prefix += "/";

            fs.bucket = bucket.substring(0, colon);
            bucket = fs.bucket;
        }

        if (flags.debugS3) {
//            awsConfig.LogLevel = aws.LogLevel(aws.LogDebug | aws.LogDebugWithRequestErrors)
//            s3Log.Level = logrus.DebugLevel
        }

        fs.awsConfig = b2Config;
//        fs.sess = session.New(awsConfig)
        fs.b2 = fs.newS3();

        boolean isAws = false;
        int err = 0;
//        if !fs.flags.RegionSet {
//            err, isAws = fs.detectBucketLocationByHEAD()
//            if err == nil {
//                // we detected a region header, this is probably AWS S3,
//                // or we can use anonymous access, or both
//                fs.sess = session.New(awsConfig)
//                fs.s3 = fs.newS3()
//            } else if err == fuse.ENOENT {
//                log.Errorf("bucket %v does not exist", fs.bucket)
//                return nil
//            } else {
//                // this is NOT AWS, we expect the request to fail with 403 if this is not
//                // an anonymous bucket
//                if err != syscall.EACCES {
//                    log.Errorf("Unable to access '%v': %v", fs.bucket, err)
//                }
//            }
//        }

        // try again with the credential to make sure
        err = fs.testBucket();
        if (err != 0) {
            if (!isAws) {
                // EMC returns 403 because it doesn't support v4 signing
                // swift3, ceph-s3 returns 400
                // Amplidata just gives up and return 500
//                if err == syscall.EACCES || err == fuse.EINVAL || err == syscall.EAGAIN {
//                    fs.fallbackV2Signer()
//                    err = mapAwsError(fs.testBucket())
//                }
            }

            if (err != 0) {
                log.error("Unable to access '%s': %s", fs.bucket, err);
                return null;
            }
        }

        new Thread(fs::cleanUpOldMPU).start();

        if (flags.useKMS) {
            //SSE header string for KMS server-side encryption (SSE-KMS)
//            fs.sseType = s3.ServerSideEncryptionAwsKms
        } else if (flags.useSSE) {
            //SSE header string for non-KMS server-side encryption (SSE-S3)
//            fs.sseType = s3.ServerSideEncryptionAes256
        }

        long now = Instant.now().getEpochSecond();
        fs.rootAttrs = new InodeAttributes();
        fs.rootAttrs.size = 4096;
        fs.rootAttrs.mTime = now;

        fs.bufferPool = new BufferPool().init();

        fs.nextInodeID = Inode.RootInodeID + 1;
        fs.inodes = new HashMap<>();
        Inode root = new Inode(fs, null, "", "");
        root.id = Inode.RootInodeID;
        root.toDir();
        root.attributes.mTime = fs.rootAttrs.mTime;

        fs.inodes.put(Inode.RootInodeID, root);

        fs.nextHandleID = 1;
        fs.dirHandles = new HashMap<>();

        fs.fileHandles = new HashMap<>();

        fs.replicators = new Ticket(16).init();
        fs.restorers = new Ticket(8).init();

//        http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
        return fs;
    }

    static int mapError(@Nullable final B2Exception e) {
        if (e == null) {
            return 0;
        }

        if (e.getStatus() == 400) {
            return -Errno.EINVAL.intValue();
        } else if (e.getStatus() == 403) {
            return -Errno.EACCES.intValue();
        } else if (e.getStatus() == 404) {
            return -Errno.ENOENT.intValue();
        } else if (e.getStatus() == 405) {
            return -3440;
        } else if (e.getStatus() == 500) {
            return -Errno.EAGAIN.intValue();
        } else {
            LogManager.getLogger("s3").error("code=%s msg=%s", e.getStatus(), e.getCode());
            return -Errno.ENOENT.intValue();
        }
    }

    String key(@Nonnull final String name) {
        return prefix + name;
    }

    // --------------------
    // Filey System Methods
    // -------------------

    // FUSE_STATFS (17)
    @Override
    @SuppressWarnings("PointlessArithmeticExpression")
    protected int statfs(@Nonnull final String path, @Nonnull final StructStatvfs op) {
        final long BLOCK_SIZE = 4096L;
        final long TOTAL_SPACE = 1L * 1024 * 1024 * 1024 * 1024 * 1024; // 1PB
        final long TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE;
        final long INODES = 1L * 1000 * 1000 * 1000; // 1 billion
        op.setSizes(1 * 1024 * 1024 /* 1MB */, BLOCK_SIZE)
                .setBlockInfo(TOTAL_BLOCKS, TOTAL_BLOCKS, TOTAL_BLOCKS)
                .setFileInfo(INODES, INODES, INODES);
        return 0;
    }

    // FUSE_GETATTR (3)
    @Override
    protected int getattr(@Nonnull final String path, @Nonnull final StructStat stat) {
        Inode inode = getInodeOrDie(lookUpInode(path));

        final FuseInodeAttributes attr = inode.getAttributes();
        if (attr != null) {
            stat.size(attr.size);
            stat.setAllTimesSec(attr.mtime);
            stat.uid(attr.uid);
            stat.gid(attr.gid);
            stat.nlink(attr.nlink);
            stat.mode(attr.mode);
            // TODO(ghart): op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL);
            return 0;
        } else {
            return -Errno.ENOENT.intValue();
        }
    }

    // FUSE_GETXATTR (22)
    @Override
    protected int getxattr(@Nonnull final String path, @Nonnull final String xattr,
                           @Nonnull final XattrFiller filler, final long size,
                           final long position) {
        // not implemented
        return super.getxattr(path, xattr, filler, size, position);
    }

    // FUSE_LISTXATTR (23)
    @Override
    protected int listxattr(@Nonnull final String path, @Nonnull final XattrListFiller filler) {
        // not implemented
        return super.listxattr(path, filler);
    }

    // FUSE_REMOVEXATTR (24)
    @Override
    protected int removexattr(@Nonnull final String path, @Nonnull final String xattr) {
        // not implemented
        return super.removexattr(path, xattr);
    }

    // FUSE_SETXATTR (21)
    @Override
    protected int setxattr(@Nonnull final String path, @Nonnull final String xattr,
                           @Nonnull final ByteBuffer buf, final long size, final int flags,
                           final int position) {
        // not implemented
        return super.setxattr(path, xattr, buf, size, flags, position);
    }

    // FUSE_LOOKUP (1) - not implemented
    // FUSE_FORGET (2) - not implemented

    // FUSE_OPENDIR (27)
    @Override
    protected int opendir(String path, StructFuseFileInfo info) {
        mu.lock();

        long handleID = nextHandleID;
        nextHandleID++;

        Inode in = getInodeOrDie(lookUpInode(path));
        mu.unlock();

        // XXX/is this a dir?
        DirHandle dh = in.openDir();

        mu.lock();
        dirHandles.put(handleID, dh);
        info.fh(handleID);
        mu.unlock();

        return 0;
    }

    // FUSE_READDIR (28)
    @Override
    protected int readdir(@Nonnull final String path, @Nonnull final StructFuseFileInfo info,
                          @Nonnull final DirectoryFiller filler) {  // TODO offset
        long bytesRead = 0;
        long offset = 0;

        // Find the handle.
        mu.lock();
        DirHandle dh = dirHandles.get(info.fh());
        mu.unlock();

        assert dh != null;

        Inode inode = dh.inode;
        inode.logFuse("ReadDir", offset);

        dh.mu.lock();

        boolean readFromS3 = false;
        List<String> dst = new ArrayList<>();

        for (long i = offset; ; ++i) {
            DirHandleEntry e;
            try {
                e = dh.readDir(i);
            } catch (NoSuchElementException err) {
                return -Errno.EINVAL.intValue();
            } catch (Exception err) {
                return -Errno.EAGAIN.intValue();
            }
            if (e == null) {
                // we've reached the end, if this was read
                // from S3 then update the cache time
                if (readFromS3) {
                    inode.dir.dirTime = Instant.now();
                    inode.attributes.mTime = inode.findChildMaxTime();
                }
                break;
            }

            if (e.inode == 0) {
                readFromS3 = true;
                insertInodeFromDirEntry(inode, e);
            }

            dst.add(makeDirEntry(e).name);

            dh.inode.logFuse("<-- ReadDir", e.name, e.offset);

            //bytesRead += n;
        }

        filler.add(dst);
        dh.mu.unlock();
        return 0;
    }

    // FUSE_RELEASEDIR (29)
    @Override
    protected int releasedir(String path, StructFuseFileInfo info) {
        mu.lock();

        DirHandle dh = dirHandles.get(info.fh());
        dh.closeDir();

        log.debug("ReleaseDirHandle %s", dh.inode.fullName());

        dirHandles.remove(info.fh());

        mu.unlock();
        return 0;
    }

    // FUSE_OPEN (14)
    @Override
    protected int open(String path, StructFuseFileInfo info) {
        /*
        fs.mu.Lock()
        in := fs.getInodeOrDie(op.Inode)
        fs.mu.Unlock()

        fh, err := in.OpenFile()
        if err != nil {
            return
        }

        fs.mu.Lock()
        defer fs.mu.Unlock()

        handleID := fs.nextHandleID
        fs.nextHandleID++

        fs.fileHandles[handleID] = fh

        op.Handle = handleID
        op.KeepPageCache = true

        return
        */
        return super.open(path, info);
    }

    // FUSE_READ (15)
    @Override
    protected int read(String path, ByteBuffer buffer, long size, long offset,
                       StructFuseFileInfo info) {
        /*
        fs.mu.Lock()
        fh := fs.fileHandles[op.Handle]
        fs.mu.Unlock()

        op.BytesRead, err = fh.ReadFile(op.Offset, op.Dst)

        return
        */
        return super.read(path, buffer, size, offset, info);
    }

    // FUSE_FSYNC (20)
    @Override
    protected int fsync(String path, int datasync, StructFuseFileInfo info) {
        /*
        // intentionally ignored, so that write()/sync()/write() works
        // see https://github.com/kahing/goofys/issues/154
        return
        */
        return super.fsync(path, datasync, info);
    }

    // FUSE_FLUSH (25)
    @Override
    protected int flush(String path, StructFuseFileInfo info) {
        // not implemented
        return super.flush(path, info);
    }

    // FUSE_RELEASE (18)
    @Override
    protected int release(String path, StructFuseFileInfo info) {
        /*
        ctx context.Context,
        op *fuseops.ReleaseFileHandleOp) (err error) {
        fs.mu.Lock()
        defer fs.mu.Unlock()

        fh := fs.fileHandles[op.Handle]
        fh.Release()

        fuseLog.Debugln("ReleaseFileHandle", *fh.inode.FullName())

        delete(fs.fileHandles, op.Handle)

        // try to compact heap
        //fs.bufferPool.MaybeGC()
        return
        */
        return super.release(path, info);
    }

    // FUSE_CREATE (35)
    @Override
    protected int create(String path, long mode, StructFuseFileInfo info) {
        // not implemented
        return super.create(path, mode, info);
    }

    // FUSE_MKDIR (9)
    @Override
    protected int mkdir(String path, long mode) {
        // not implemented
        return super.mkdir(path, mode);
    }

    // FUSE_RMDIR (11)
    @Override
    protected int rmdir(String path) {
        return super.rmdir(path);
    }

    // FUSE_SETATTR (4) - not implemented

    // FUSE_WRITE (16)
    @Override
    protected int write(String path, ByteBuffer buf, long bufSize, long writeOffset,
                        StructFuseFileInfo wrapper) {
        // not implemented
        return super.write(path, buf, bufSize, writeOffset, wrapper);
    }

    // FUSE_UNLINK (10)
    @Override
    protected int unlink(String path) {
        // not implemented
        return super.unlink(path);
    }

    // FUSE_RENAME (12)
    @Override
    protected int rename(String path, String newName) {
        // not implemented
        return super.rename(path, newName);
    }

    // ------------------------
    // Non-Filey System Methods
    // ------------------------

    @Override
    protected int mknod(String path, long mode, long dev) {
        // not supported
        return -Errno.ENOSYS.intValue();
    }

    @Override
    protected int symlink(String path, String target) {
        // not supported
        return -Errno.ENOSYS.intValue();
    }

    @Override
    protected int readlink(String path, ByteBuffer buffer, long size) {
        // not supported
        return -Errno.ENOSYS.intValue();
    }

    // ----------------
    // Internal Methods
    // ----------------

    long allocateInodeId() {
        long id = nextInodeID;
        nextInodeID++;
        return id;
    }

    int cleanUpOldMPU() {
        B2ListFilesIterable mpu;
        try {
            mpu = b2.unfinishedLargeFiles(bucket);
        } catch (B2Exception e) {
            return mapError(e);
        }
        LogManager.getLogger("s3").debug(mpu);

        long now = Instant.now().getEpochSecond();
        for (B2FileVersion upload : mpu) {
            long expireTime = upload.getUploadTimestamp() + (48 * 3600);

            if (expireTime <= now) {
                try {
                    b2.cancelLargeFile(upload.getFileId());
                } catch (B2Exception e) {
                    if (mapError(e) == -Errno.EACCES.intValue()) {
                        break;
                    }
                }
            } else {
                LogManager.getLogger("s3").debug("Keeping MPU Key=%s Id=%s",
                                                 upload.getFileName(), upload.getFileId());
            }
        }

        return 0;
    }

    static boolean expired(@Nonnull final Instant cache, @Nonnull final Duration ttl) {
        return cache.plus(ttl).isAfter(Instant.now());
    }

    /**
     * Find the given inode. Panic if it doesn't exist.
     */
    synchronized Inode getInodeOrDie(final long id) {
        final Inode inode = inodes.get(id);
        assert inode != null;
        return inode;
    }

    synchronized void insertInode(Inode parent, Inode inode) {
        inode.id = allocateInodeId();
        parent.insertChildUnlocked(inode);
        inodes.put(inode.id, inode);
        paths.put(inode.fullName(), inode.id);
    }

    Inode insertInodeFromDirEntry(Inode parent, DirHandleEntry entry) {
        Inode inode;

        parent.mu.lock();

        inode = parent.findChildUnlocked(entry.name, entry.type == DT_Directory);
        if (inode == null) {
            String path = parent.getChildName(entry.name);
            inode = new Inode(this, parent, entry.name, path);
            if (entry.type == DT_Directory) {
                inode.toDir();
            } else {
                inode.attributes = entry.attributes;
            }
            if (entry.eTag != null) {
                inode.s3Metadata.put("etag", ByteBuffer.wrap(entry.eTag.getBytes()));
            }
            if (entry.storageClass != null) {
                inode.s3Metadata.put("storage-class",
                                     ByteBuffer.wrap(entry.storageClass.getBytes()));
            }
            // there are fake dir entries, we will realize the refcnt when
            // lookup is done
            inode.refcnt = 0;

            insertInode(parent, inode);
        } else {
            inode.mu.lock();

            if (entry.eTag != null) {
                inode.s3Metadata.put("etag", ByteBuffer.wrap(entry.eTag.getBytes()));
            }
            if (entry.storageClass != null) {
                inode.s3Metadata.put("storage-class",
                                     ByteBuffer.wrap(entry.storageClass.getBytes()));
            }
            inode.knownSize = entry.attributes.size;
            inode.attributes.mTime = entry.attributes.mTime;
            inode.attrTime = Instant.now();

            inode.mu.unlock();
        }

        parent.mu.unlock();
        return inode;
    }

    synchronized long lookUpInode(@Nonnull final String path) {
        if (paths.containsKey(path)) {
            return paths.get(path);
        }

        // LookUpInodeOp
        final String opName = path.substring(path.lastIndexOf('/') + 1);
        final long opParent = lookUpInode(path.substring(0, path.lastIndexOf('/')));

        // LookUpInode
        Inode inode;
        boolean ok;
        log.debug("<-- LookUpInode %s %s %s", opParent, opName, "");

        Inode parent = getInodeOrDie(opParent);

        parent.mu.lock();
        inode = parent.findChildUnlockedFull(opName);
        if (inode != null) {
            ok = true;
            inode.ref();

            if (expired(inode.attrTime, flags.statCacheTtl)) {
                ok = false;
                if (inode.fileHandles != 0) {
                    // we have an open file handle, object
                    // in S3 may not represent the true
                    // state of the file anyway, so just
                    // return what we know which is
                    // potentially more accurate
                    ok = true;
                } else {
                    inode.logFuse("lookup expired");
                }
            }
        } else {
            ok = false;
        }
        parent.mu.unlock();

        if (!ok) {
            Inode newInode;

            try {
                newInode = parent.lookUp(opName);
            } catch (Exception e) {
                if (inode != null) {
                    // just kidding! pretend we didn't up the ref
                    boolean stale = inode.deRef(1);
                    if (stale) {
                        inodes.remove(inode.id);
                        paths.remove(inode.fullName());
                        parent.removeChild(inode);
                    }
                }
                throw new RuntimeException(e);
            }

            if (inode == null) {
                parent.mu.lock();
                // check again if it's there, could have been
                // added by another lookup or readdir
                inode = parent.findChildUnlockedFull(opName);
                if (inode == null) {
                    inode = newInode;
                    insertInode(parent, inode);
                }
                parent.mu.unlock();
            } else {
                inode.attributes = newInode.attributes;
                inode.attrTime = Instant.now();
            }
        }

        return inode.id;
    }

    static Dirent makeDirEntry(DirHandleEntry en) {
        Dirent d = new Dirent();
        d.name = en.name;
        d.type = en.type;
        d.inode = RootInodeID + 1;
        d.offset = en.offset;
        return d;
    }

    B2StorageClient newS3() {
        B2StorageClient svc = B2StorageHttpClientBuilder.builder(awsConfig).build();
        return svc;
    }

    int testBucket() {
        String randomObjectName = key(UUID.randomUUID().toString());

        int err = 0;
        try {
            b2.fileNames(
                    B2ListFileNamesRequest.builder(bucket).setPrefix(randomObjectName).build());
        } catch (B2Exception e) {
            err = mapError(e);
            if (err == -Errno.ENOENT.intValue()) {
                err = 0;
            }
        }
        return err;
    }
}
