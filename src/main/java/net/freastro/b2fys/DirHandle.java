package net.freastro.b2fys;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;

import org.apache.logging.log4j.LogManager;

import sun.awt.Mutex;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static net.freastro.b2fys.syscall.DT_Directory;
import static net.freastro.b2fys.syscall.DT_File;

class DirHandle {

    Inode inode;

    Mutex mu; // everything below is protected by mu
    DirHandleEntry[] entries;
    String marker;
    int baseOffset;

    DirHandle(Inode inode) {
        this.inode = inode;
    }

    int closeDir() {
        return 0;
    }

    B2ListFilesIterable listObjects(String prefix) {
        Future<B2ListFilesIterable> slurpChan = null;
        Future<B2ListFilesIterable> listChan = null;

        B2FuseFilesystem fs = inode.fs;

        // try to list without delimiter to see if we have to slurp up
        // multiple directories
        if (marker == null && !fs.flags.TypeCacheTtl.isZero()
            && (inode.parent != null && inode.parent.dir.seqOpenDirScore >= 2)) {
            slurpChan = CompletableFuture.supplyAsync(() -> listObjectsSlurp(prefix));
        } else {
            slurpChan = null;
        }

        Supplier<B2ListFilesIterable> listObjectsFlat = () -> {
            final B2ListFileNamesRequest params = B2ListFileNamesRequest.builder(fs.bucket)
                    .setDelimiter("/").setStartFileName(marker).setPrefix(prefix).build();
            try {
                return fs.b2.fileNames(params);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        if (!fs.flags.cheap) {
            // invoke the fallback in parallel if desired
            listChan = CompletableFuture.supplyAsync(listObjectsFlat);
        }

        // first see if we get anything from the slurp
        if (slurpChan != null) {
            try {
                return slurpChan.get();
            } catch (Exception err) {
                // ignored
            }
        }

        if (fs.flags.cheap) {
            listChan = CompletableFuture.supplyAsync(listObjectsFlat);
        }

        // if we got an error (which may mean slurp is not applicable,
        // wait for regular list
        try {
            return listChan.get();
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    B2ListFilesIterable listObjectsSlurp(String prefix) {
        B2ListFilesIterable resp;

        String marker = null;
        String reqPrefix = prefix;
        Inode inode = this.inode;
        B2FuseFilesystem fs = inode.fs;
        if (this.inode.parent != null) {
            inode = this.inode.parent;
            reqPrefix = fs.key(inode.fullName());
            if (inode.fullName().length() != 0) {
                reqPrefix += "/";
            }
            marker = fs.key(this.inode.fullName() + "/");
        }

        final B2ListFileNamesRequest params = B2ListFileNamesRequest.builder(fs.bucket)
                .setPrefix(reqPrefix).setStartFileName(marker).build();

        try {
            resp = fs.b2.fileNames(params);
        } catch (Exception err) {
            LogManager.getLogger("s3").error("ListObjects %s = %s", params, err);
            throw new RuntimeException();
        }

        int num = (resp.iterator().hasNext()) ? 1 : 0;
        if (num == 0) {
            return resp;
        }

        Map<Inode, Boolean> dirs = new HashMap<>();
        for (B2FileVersion obj : resp) {
            String baseName = obj.getFileName().substring(reqPrefix.length());

            int slash = baseName.indexOf('/');
            if (slash != -1) {
                inode.insertSubTree(baseName, obj, dirs);
            }
        }

        for (Map.Entry<Inode, Boolean> entry : dirs.entrySet()) {
            Inode d = entry.getKey();
            Boolean sealed = entry.getValue();

            if (d.equals(this.inode)) {
                // never seal the current dir because that's
                // handled at upper layer
                continue;
            }

            if (sealed /* || resp.IsTruncated */) {
                d.dir.dirTime = Instant.now();
                d.attributes.mTime = d.findChildMaxTime();
            }
        }

        /*
        if *resp.IsTruncated {
            obj := resp.Contents[len(resp.Contents)-1]
            // if we are done listing prefix, we are good
            if strings.HasPrefix(*obj.Key, prefix) {
                // if we are done with all the slashes, then we are good
                baseName := (*obj.Key)[len(prefix):]

                for _, c := range baseName {
                    if c <= '/' {
                        // if an entry is ex: a!b, then the
                        // next entry could be a/foo, so we
                        // are not done yet.
                        resp = nil
                        break
                    }
                }
            }
        }
        */

        // we only return this response if we are totally done with listing this dir
        /*
        if resp != nil {
            resp.IsTruncated = aws.Bool(false)
            resp.NextMarker = nil
        }
        */

        return resp;
    }

    static DirHandleEntry objectToDirEntry(B2FuseFilesystem fs, B2FileVersion obj, String name,
                                           boolean isDir) {
        DirHandleEntry en;

        if (isDir) {
            en = new DirHandleEntry();
            en.name = name;
            en.type = DT_Directory;
            en.attributes = fs.rootAttrs;
        } else {
            en = new DirHandleEntry();
            en.name = name;
            en.type = DT_File;
            en.attributes = new InodeAttributes();
            en.attributes.size = obj.getContentLength();
            en.attributes.mTime = obj.getUploadTimestamp();
            en.eTag = obj.getContentSha1();
            en.storageClass = null;
        }

        return en;
    }

    DirHandleEntry readDir(long offset) {
        // If the request is for offset zero, we assume that either this is the first
        // call or rewinddir has been called.
        if (offset == 0) {
            entries = null;
        }

        DirHandleEntry en;
        try {
            en = inode.readDirFromCache(offset);
            return en;
        } catch (IllegalStateException e) {
            // ignored
        }

        B2FuseFilesystem fs = inode.fs;

        if (offset == 0) {
            en = new DirHandleEntry();
            en.name = ".";
            en.type = DT_Directory;
            en.attributes = fs.rootAttrs;
            en.offset = 1;
            return en;
        } else if (offset == 1) {
            en = new DirHandleEntry();
            en.name = "..";
            en.type = DT_Directory;
            en.attributes = fs.rootAttrs;
            en.offset = 2;
            return en;
        }

        int i = ((int) offset) - baseOffset - 2;
        assert i >= 0;

        if (i >= entries.length) {
            if (marker != null) {
                // we need to fetch the next page
                entries = null;
                baseOffset += i;
                i = 0;
            }
        }

        if (entries == null) {
            // try not to hold the lock when we make the request
            mu.unlock();

            String prefix = fs.key(inode.fullName());
            if (inode.fullName().length() != 0) {
                prefix += "/";
            }

            B2ListFilesIterable resp;
            try {
                resp = listObjects(prefix);
            } finally {
                mu.lock();
            }

            LogManager.getLogger("s3").debug(resp);
            mu.lock();

            List<DirHandleEntry> entries = new ArrayList<>();

            // this is only returned for non-slurped responses
            /*
            for _, dir := range resp.CommonPrefixes {
                // strip trailing /
                dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
                // strip previous prefix
                dirName = dirName[len(*resp.Prefix):]
                if len(dirName) == 0 {
                    continue
                }
                en = &DirHandleEntry{
                    Name:       &dirName,
                            Type:       fuseutil.DT_Directory,
                            Attributes: &fs.rootAttrs,
                }

                dh.Entries = append(dh.Entries, en)
            }
            */

            String lastDir = "";
            for (B2FileVersion obj : resp) {
                if (!obj.getFileName().startsWith(prefix)) {
                    // other slurped objects that we cached
                    continue;
                }

                String baseName = obj.getFileName().substring(prefix.length());

                int slash = baseName.indexOf('/');
                if (slash == -1) {
                    if (baseName.length() == 0) {
                        // shouldn't happen
                        continue;
                    }
                    this.entries = Arrays.copyOf(this.entries, this.entries.length + 1);
                    this.entries[this.entries.length - 1] =
                            objectToDirEntry(fs, obj, baseName, false);
                } else {
                    // this is a slurped up object which
                    // was already cached, unless it's a
                    // directory right under this dir that
                    // we need to return
                    String dirName = baseName.substring(0, slash);
                    if (!dirName.equals(lastDir) && !lastDir.equals("")) {
                        // make a copy so we can take the address
                        String dir = lastDir;
                        DirHandleEntry en1 = new DirHandleEntry();
                        en1.name = dir;
                        en1.type = DT_Directory;
                        en1.attributes = fs.rootAttrs;
                        this.entries = Arrays.copyOf(this.entries, this.entries.length + 1);
                        this.entries[this.entries.length - 1] = en1;
                    }
                    lastDir = dirName;
                }
            }
            if (!lastDir.equals("")) {
                en = new DirHandleEntry();
                en.name = lastDir;
                en.type = DT_Directory;
                en.attributes = fs.rootAttrs;
                this.entries = Arrays.copyOf(this.entries, this.entries.length + 1);
                this.entries[this.entries.length - 1] = en;
            }

            Arrays.sort(this.entries, Comparator.comparing(o -> o.name));

            // Fix up offset fields.
            for (int i1 = 0; i1 < this.entries.length; i1++) {
                en = this.entries[i1];
                // offset is 1 based, also need to account for "." and ".."
                en.offset = (i1+baseOffset) + 1 + 2;
            }

            /*
            if *resp.IsTruncated {
                dh.Marker = resp.NextMarker
            } else {
                dh.Marker = nil
            }
            */
        }

        if (i == entries.length) {
            // we've reached the end
            return null;
        } else if (i > entries.length) {
            throw new NoSuchElementException();
        }

        return entries[i];
    }
}
