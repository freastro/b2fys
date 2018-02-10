package net.freastro.b2fys;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.awt.Mutex;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import co.paralleluniverse.fuse.TypeMode;

import static net.freastro.b2fys.syscall.DT_Directory;
import static net.freastro.b2fys.syscall.DT_File;

class Inode {

    private static final Logger log = LogManager.getLogger(Inode.class);

    static long RootInodeID = 1;

    long id;
    String name;
    B2FuseFilesystem fs;
    InodeAttributes attributes = new InodeAttributes();
    long knownSize;
    Instant attrTime;

    Mutex mu = new Mutex(); // everything below is protected by mu

    Inode parent;

    DirInodeData dir;

    boolean invalid;
    boolean implicitDir;

    int fileHandles;

    Map<String, ByteBuffer> userMetadata;
    Map<String, ByteBuffer> s3Metadata;

    // the refcnt is an exception, it's protected by the global lock
    long refcnt;

    Inode(B2FuseFilesystem fs, Inode parent, String name, String fullName) {
        this.name = name;
        this.fs = fs;
        this.attrTime = Instant.now();
        this.parent = parent;
        this.s3Metadata = new HashMap<>();
        this.refcnt = 1;
    }

    void addDotAndDotDot() {
        B2FuseFilesystem fs = this.fs;
        DirHandleEntry en = new DirHandleEntry();
        en.name = ".";
        en.type = DT_Directory;
        en.attributes = fs.rootAttrs;
        en.offset = 1;
        fs.insertInodeFromDirEntry(this, en);
        en = new DirHandleEntry();
        en.name = "..";
        en.type = DT_Directory;
        en.attributes = fs.rootAttrs;
        en.offset = 2;
        fs.insertInodeFromDirEntry(this, en);
    }

    synchronized boolean deRef(long n) {
        logFuse("DeRef", n, refcnt);

        assert (refcnt >= n);

        refcnt -= n;

        return (refcnt == 0);
    }

    void fillXattrFromHead(B2FileVersion resp) {
        userMetadata = new HashMap<>();

        if (resp.getContentSha1() != null) {
            s3Metadata.put("etag", ByteBuffer.wrap(resp.getContentSha1().getBytes()));
        }
    }

    int findChildIdxUnlocked(String name) {
        int l = this.dir.children.length;
        if (l == 0) {
            return -1;
        }
        int i = sortSearch(l, this.findInodeFunc(name, true));
        if (i < l) {
            // found
            if (this.dir.children[i].name.equals(name)) {
                return i;
            }
        }
        return -1;
    }

    long findChildMaxTime() {
        long maxTime = this.attributes.mTime;

        for (int i = 0; i < this.dir.children.length; ++i) {
            Inode c = this.dir.children[i];
            if (c.attributes.mTime > maxTime) {
                maxTime = c.attributes.mTime;
            }
        }

        return maxTime;
    }

    Inode findChildUnlocked(@Nonnull final String name, final boolean isDir) {
        int l = (this.dir.children != null) ? this.dir.children.length : 0;
        if (l == 0) {
            return null;
        }
        int i = sortSearch(l, this.findInodeFunc(name, isDir));
        if (i < l) {
            // found
            if (this.dir.children[i].name.equals(name)) {
                return this.dir.children[i];
            }
        }
        return null;
    }

    Inode findChildUnlockedFull(@Nonnull final String name) {
        Inode inode = this.findChildUnlocked(name, false);
        if (inode == null) {
            inode = this.findChildUnlocked(name, true);
        }
        return inode;
    }

    Predicate<Integer> findInodeFunc(@Nonnull final String name, final boolean isDir) {
        // sort dirs first, then by name
        return i -> {
            if (this.dir.children[i].isDir() != isDir) {
                return isDir;
            }
            return this.dir.children[i].name.compareTo(name) >= 0;
        };
    }

    String fullName() {
        if (parent == null) {
            return name;
        } else {
            return parent.getChildName(name);
        }
    }

    FuseInodeAttributes getAttributes() {
        // XXX refresh attributes
        logFuse("GetAttributes");
        if (invalid) {
            return null;
        }
        return inflateAttributes();
    }

    String getChildName(final String name) {
        if (id == RootInodeID) {
            return name;
        } else {
            return String.format("%s/%s", fullName(), name);
        }
    }

    FuseInodeAttributes inflateAttributes() {
        final FuseInodeAttributes attr = new FuseInodeAttributes();
        attr.size = attributes.size;
        attr.atime = attributes.mTime;
        attr.mtime = attributes.mTime;
        attr.ctime = attributes.mTime;
        attr.crtime = attributes.mTime;
        attr.uid = fs.flags.uid;
        attr.gid = fs.flags.gid;

        if (dir != null) {
            attr.nlink = 2;
            attr.mode = fs.flags.dirMode | TypeMode.S_IFDIR;
        } else {
            attr.nlink = 1;
            attr.mode = fs.flags.fileMode | TypeMode.S_IFREG;
        }
        return attr;
    }

    void insertChildUnlocked(Inode inode) {
        int l = (this.dir.children != null) ? this.dir.children.length : 0;
        if (l == 0) {
            this.dir.children = new Inode[]{inode};
            return;
        }

        int i = sortSearch(l, this.findInodeFunc(inode.name, inode.isDir()));
        if (i == l) {
            // not found = new value is the biggest
            this.dir.children = Stream.concat(Arrays.stream(this.dir.children), Stream.of(inode))
                    .toArray(Inode[]::new);
        } else {
            assert !(this.dir.children[i].name.equals(inode.name));

            this.dir.children = Stream.concat(Arrays.stream(this.dir.children), Stream.of(inode))
                    .toArray(Inode[]::new);
            System.arraycopy(this.dir.children, i, this.dir.children, i + 1,
                             this.dir.children.length - i - 1);
            this.dir.children[i] = inode;
        }
    }

    void insertSubTree(String path, B2FileVersion obj, Map<Inode, Boolean> dirs) {
        B2FuseFilesystem fs = this.fs;
        int slash = path.indexOf('/');
        if (slash == -1) {
            fs.insertInodeFromDirEntry(this, DirHandle.objectToDirEntry(fs, obj, path, false));
            sealPastDirs(dirs, this);
        } else {
            String dir = path.substring(0, slash);
            path = path.substring(slash + 1);

            if (path.length() == 0) {
                Inode inode = fs.insertInodeFromDirEntry(
                        this, DirHandle.objectToDirEntry(fs, obj, dir, true));
                inode.addDotAndDotDot();

                sealPastDirs(dirs, inode);
            } else {
                // ensure that the potentially implicit dir is added
                DirHandleEntry en = new DirHandleEntry();
                en.name = dir;
                en.type = DT_Directory;
                en.attributes = fs.rootAttrs;
                en.offset = 1;
                Inode inode = fs.insertInodeFromDirEntry(this, en);
                // mark this dir but don't seal anything else
                // until we get to the leaf
                dirs.put(inode, false);

                inode.addDotAndDotDot();
                inode.insertSubTree(path, obj, dirs);
            }
        }
    }

    boolean isDir() {
        return dir != null;
    }

    // if I had seen a/ and a/b, and now I get a/c, that means a/b is
    // done, but not a/
    boolean isParentOf(Inode inode) {
        return inode.parent != null && (this.equals(inode.parent) || this.isParentOf(inode.parent));
    }

    void logFuse(final String op, final Object... args) {
        log.debug(op + ',' + id + ',' + fullName() + ',' + Arrays.toString(args));
    }

    Inode lookUp(@Nonnull final String name) {
        this.logFuse("Inode.LookUp", name);

        return this.lookUpInodeMaybeDir(name, this.getChildName(name));
    }

    CompletableFuture<B2ListFilesIterable> lookUpInodeDir(@Nonnull final String name) {
        return CompletableFuture.supplyAsync(() -> {
            B2ListFileNamesRequest params = B2ListFileNamesRequest.builder(this.fs.bucket)
                    .setDelimiter("/")
                    .setMaxFileCount(1)
                    .setPrefix(this.fs.key(name + "/"))
                    .build();

            B2ListFilesIterable resp;
            try {
                resp = this.fs.b2.fileNames(params);
            } catch (B2Exception e) {
                throw new RuntimeException(e);
            }

            LogManager.getLogger("s3").debug(resp);
            return resp;
        });
    }

    // returned inode has nil id
    Inode lookUpInodeMaybeDir(@Nonnull final String name, @Nonnull final String fullName) {
        Future<B2FileVersion> objectChan = null;
        Future<B2FileVersion> dirBlobChan = null;
        Future<B2ListFilesIterable> dirChan = null;

        int checking = 3;
        Exception[] checkErr = new Exception[3];

        assert this.fs.b2 != null;

        objectChan = this.lookUpInodeNotDir(fullName);
        if (!this.fs.flags.cheap) {
            dirBlobChan = this.lookUpInodeNotDir(fullName + "/");
            if (!this.fs.flags.explicitDir) {
                dirChan = this.lookUpInodeDir(fullName);
            }
        }

        while (true) {
            try {
                CompletableFuture.anyOf(Stream.of(objectChan, dirBlobChan, dirChan)
                                                .filter(Objects::nonNull)
                                                .toArray(CompletableFuture[]::new))
                        .get();
            } catch (Exception e) {
                // ignored
            }

            if (objectChan != null && objectChan.isDone()) {
                try {
                    B2FileVersion resp = objectChan.get();
                    // XXX/TODO if both object and object/ exists, return dir
                    Inode inode = new Inode(this.fs, this, name, fullName);
                    inode.attributes = new InodeAttributes();
                    inode.attributes.size = resp.getContentLength();
                    inode.attributes.mTime = resp.getUploadTimestamp() / 1000;

                    // don't want to print to the attribute because that
                    // can get updated
                    long size = inode.attributes.size;
                    inode.knownSize = size;

                    inode.fillXattrFromHead(resp);
                    return inode;
                } catch (Exception err) {
                    checking--;
                    checkErr[0] = err;
                    LogManager.getLogger("s3").debug("HEAD %s = %s", fullName, err);
                }
                objectChan = null;
            }

            if (dirChan != null && dirChan.isDone()) {
                try {
                    B2ListFilesIterable resp = dirChan.get();
                    Iterator<B2FileVersion> iter = resp.iterator();
                    if (iter.hasNext()) {
                        Inode inode = new Inode(this.fs, this, name, fullName);
                        inode.toDir();
                        B2FileVersion entry = iter.next();
                        if (entry.getFileName().equals(name + "/")) {
                            if (entry.getContentSha1() != null) {
                                inode.s3Metadata.put("etag",
                                                     ByteBuffer.wrap(entry.getContentSha1()
                                                                             .getBytes()));
                            }
                        }
                        // if cheap is not on, the dir blob
                        // could exist but this returned first
                        if (fs.flags.cheap) {
                            implicitDir = true;
                        }
                        return inode;
                    }
                } catch (Exception err) {
                    checking--;
                    checkErr[2] = err;
                    LogManager.getLogger("s3").debug("LIST %s/ = %s", fullName, err);
                }
                dirChan = null;
            }

            if (dirBlobChan != null && dirBlobChan.isDone()) {
                try {
                    B2FileVersion resp = dirBlobChan.get();
                    Inode inode = new Inode(this.fs, this, name, fullName);
                    inode.toDir();
                    inode.fillXattrFromHead(resp);
                    return inode;
                } catch (Exception err) {
                    checking--;
                    checkErr[1] = err;
                    LogManager.getLogger("s3").debug("HEAD %s/ = %s", fullName, err);
                }
                dirBlobChan = null;
            }

            if (checking == 2) {
                if (this.fs.flags.cheap) {
                    dirBlobChan = this.lookUpInodeNotDir(fullName + "/");
                }
            }
            if (checking == 1) {
                if (this.fs.flags.explicitDir) {
                    checkErr[2] = new RuntimeException();
                    checking = 0;
                } else if (this.fs.flags.cheap) {
                    dirChan = this.lookUpInodeDir(fullName);
                }
            }
            if (checking == 0) {
                throw new RuntimeException();
            }
        }
    }

    CompletableFuture<B2FileVersion> lookUpInodeNotDir(@Nonnull final String name) {
        return CompletableFuture.supplyAsync(() -> {
            B2ListFileNamesRequest params = B2ListFileNamesRequest.builder(this.fs.bucket)
                    .setDelimiter("/")
                    .setMaxFileCount(1)
                    .setPrefix(this.fs.key(name))
                    .build();

            B2ListFilesIterable resp;
            try {
                resp = this.fs.b2.fileNames(params);
            } catch (B2Exception e) {
                throw new RuntimeException(e);
            }
            LogManager.getLogger("s3").debug(resp);

            return resp.iterator().next();
        });
    }

    DirHandleEntry readDirFromCache(long offset) {
        mu.lock();
        DirHandleEntry en = null;
        boolean ok = false;

        if (!B2FuseFilesystem.expired(this.dir.dirTime, this.fs.flags.TypeCacheTtl)) {
            ok = true;

            if (this.dir.children == null || offset >= this.dir.children.length) {
                // return
            } else {
                Inode child = this.dir.children[(int) offset];

                en = new DirHandleEntry();
                en.name = child.name;
                en.inode = child.id;
                en.offset = offset + 1;
                en.attributes = child.attributes;
                if (child.isDir()) {
                    en.type = DT_Directory;
                } else {
                    en.type = DT_File;
                }
            }
        }

        mu.unlock();
        if (ok) {
            return en;
        } else {
            throw new IllegalStateException();
        }
    }

    DirHandle openDir() {
        logFuse("OpenDir");

        Inode parent = this.parent;
        if (parent != null && !fs.flags.TypeCacheTtl.isZero()) {
            parent.mu.lock();

            int num = parent.dir.children.length;

            if (parent.dir.lastOpenDir == null && num > 0
                && parent.dir.children[0].name.equals(name)) {
                if (parent.dir.seqOpenDirScore < 255) {
                    parent.dir.seqOpenDirScore += 1;
                }
                // 2.1) if I open a/a, a/'s score is now 2
                // ie: handle the depth first search case
                if (parent.dir.seqOpenDirScore >= 2) {
                    log.debug("%s in readdir mode", parent.fullName());
                }
            } else if (parent.dir.lastOpenDir != null && parent.dir.lastOpenDirIdx + 1 < num &&
                       // we are reading the next one as expected
                       parent.dir.children[parent.dir.lastOpenDirIdx + 1].name.equals(name) &&
                       // check that inode positions haven't moved
                       parent.dir.children[parent.dir.lastOpenDirIdx].name
                               .equals(parent.dir.lastOpenDir)) {
                // 2.2) if I open b/, root's score is now 2
                // ie: handle the breath first search case
                if (parent.dir.seqOpenDirScore < 255) {
                    parent.dir.seqOpenDirScore++;
                }
                parent.dir.lastOpenDirIdx += 1;
                if (parent.dir.seqOpenDirScore == 2) {
                    log.debug("%s in readdir mode", parent.fullName());
                }
            } else {
                parent.dir.seqOpenDirScore = 0;
                parent.dir.lastOpenDirIdx = parent.findChildIdxUnlocked(name);
                assert (parent.dir.lastOpenDirIdx != -1);
            }

            parent.dir.lastOpenDir = name;
            mu.lock();
            if (dir.lastOpenDir == null) {
                // 1) if I open a/, root's score = 1 (a is the first dir),
                // so make a/'s count at 1 too
                dir.seqOpenDirScore = parent.dir.seqOpenDirScore;
                if (dir.seqOpenDirScore >= 2) {
                    log.debug("%s in readdir mode", fullName());
                }
            }
            mu.unlock();

            parent.mu.unlock();
        }

        DirHandle dh = new DirHandle(this);
        return dh;
    }

    // LOCKS_REQUIRED(fs.mu)
    // XXX why did I put lock required? This used to return a resurrect bool
    // which no long does anything, need to look into that to see if
    // that was legacy
    synchronized void ref() {
        logFuse("Ref", refcnt);

        refcnt++;
    }

    synchronized void removeChild(Inode inode) {
        this.removeChildUnlocked(inode);
    }

    void removeChildUnlocked(Inode inode) {
        int l = this.dir.children.length;
        if (l == 0) {
            return;
        }
        int i = sortSearch(l, this.findInodeFunc(inode.name, inode.isDir()));
        assert i < l && this.dir.children[i].name.equals(inode.name);

        System.arraycopy(this.dir.children, i + 1, this.dir.children, i, i);
        this.dir.children[l - 1] = null;
        this.dir.children = Arrays.copyOf(this.dir.children, this.dir.children.length - 2);
    }

    static void sealPastDirs(Map<Inode, Boolean> dirs, Inode d) {
        for (Map.Entry<Inode, Boolean> entry : dirs.entrySet()) {
            Inode p = entry.getKey();
            Boolean sealed = entry.getValue();
            if (!p.equals(d) && !sealed && !p.isParentOf(d)) {
                dirs.put(p, true);
            }
        }
        // I just read something in d, obviously it's not done yet
        dirs.put(d, false);
    }

    void toDir() {
        attributes = fs.rootAttrs;
        dir = new DirInodeData();
        knownSize = fs.rootAttrs.size;
    }

    // TODO(ghart)
    private int sortSearch(final int n, @Nonnull final Predicate<Integer> f) {
        for (int i = 0; i < n; ++i) {
            if (f.test(i)) {
                return i;
            }
        }
        return n;
    }
}
