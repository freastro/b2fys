package net.freastro.b2fys;

import com.backblaze.b2.client.B2ClientConfig;
import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentHandlers.B2ContentSink;
import com.backblaze.b2.client.contentSources.B2Headers;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import co.paralleluniverse.fuse.DirectoryFiller;
import co.paralleluniverse.fuse.StructFuseFileInfo;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;

public class B2FuseFilesystemTest {

    /**
     * Name of the test bucket
     */
    private static final String BUCKET = "BUCKET";

    /**
     * Path for the mount point
     */
    private static final String MOUNT_POINT = "/mnt";

    /**
     * Test getting attributes.
     */
    @Test
    public void testGetAttr() throws Exception {
        // Mock B2 client
        final B2StorageClient b2 = createB2StorageClient();
        Mockito.when(b2.fileNames(Mockito.any(B2ListFileNamesRequest.class))).then(answer -> {
            final B2ListFileNamesRequest request = answer.getArgument(0);
            if (request.getPrefix().equals("file")) {
                return (B2ListFilesIterable) () -> {
                    final B2FileVersion file = new B2FileVersion(
                            "100", "file", 42, "text/plain", "",
                            Collections.emptyMap(), "", 515196900000L);
                    return Collections.singletonList(file).iterator();
                };
            } else {
                return null;
            }
        });

        // Mock filesystem
        final B2FuseFilesystem fs = createFilesystem(b2);

        // Test getattr "/"
        MockStructStat stat = new MockStructStat("/");
        int result = fs.getattr(stat.path(), stat);
        Assert.assertEquals(0, result);
        Assert.assertEquals(0, stat.uid());
        Assert.assertEquals(0, stat.gid());
        Assert.assertEquals(16877, stat.mode());
        Assert.assertEquals(2, stat.nlink());
        Assert.assertEquals(4096, stat.size());

        // Test getattr "/test"
        stat = new MockStructStat("/file");
        result = fs.getattr(stat.path(), stat);
        Assert.assertEquals(0, result);
        Assert.assertEquals(0, stat.uid());
        Assert.assertEquals(0, stat.gid());
        Assert.assertEquals(515196900, stat.atime());
        Assert.assertEquals(515196900, stat.mtime());
        Assert.assertEquals(515196900, stat.ctime());
        Assert.assertEquals(33188, stat.mode());
        Assert.assertEquals(1, stat.nlink());
        Assert.assertEquals(42, stat.size());
    }

    /**
     * Test reading directory entries.
     */
    @Test
    public void testReadDir() throws Exception {
        // Create B2 client
        final B2StorageClient b2 = createB2StorageClient();
        Mockito.when(b2.fileNames(Mockito.any(B2ListFileNamesRequest.class))).then(answer -> {
            final B2ListFileNamesRequest request = answer.getArgument(0);
            if (request.getPrefix().equals("")) {
                return (B2ListFilesIterable) () -> {
                    final B2FileVersion file = new B2FileVersion(
                            "100", "test", 0, "text/plain", "",
                            Collections.emptyMap(), "", 0L);
                    return Collections.singletonList(file).iterator();
                };
            } else {
                return null;
            }
        });

        // Test opendir
        final B2FuseFilesystem fs = createFilesystem(b2);

        final StructFuseFileInfo info = createFileInfo("/");
        int result = fs.opendir("/", info);
        Assert.assertEquals(0, result);

        // Test readdir
        final DirectoryFiller directoryFiller = Mockito.mock(DirectoryFiller.class);
        Mockito.when(directoryFiller.add(Mockito.any())).then(answer -> {
            final List<String> list = new ArrayList<>(answer.getArgument(0));
            Assert.assertEquals(3, list.size());
            return null;
        });
        result = fs.readdir("/", info, directoryFiller);
        Assert.assertEquals(0, result);
        Mockito.verify(directoryFiller, Mockito.times(1)).add(Mockito.any());

        // Test releasedir
        result = fs.releasedir("/", info);
        Assert.assertEquals(0, result);
    }

    /**
     * Test reading file entries.
     */
    @Test
    public void testReadFile() throws Exception {
        // Create B2 client
        final B2StorageClient b2 = createB2StorageClient();
        Mockito.doAnswer(answer -> {
            final B2DownloadByNameRequest request = answer.getArgument(0);
            final B2ContentSink sink = answer.getArgument(1);
            if (request.getFileName().equals("test")) {
                final InputStream stream = new ByteArrayInputStream("Hello, world!".getBytes());
                sink.readContent(Mockito.mock(B2Headers.class), stream);
            }
            return null;
        }).when(b2).downloadByName(Mockito.any(B2DownloadByNameRequest.class),
                                   Mockito.any(B2ContentSink.class));
        Mockito.when(b2.fileNames(Mockito.any(B2ListFileNamesRequest.class))).then(answer -> {
            final B2ListFileNamesRequest request = answer.getArgument(0);
            if (request.getPrefix().equals("") || request.getPrefix().equals("test")) {
                return (B2ListFilesIterable) () -> {
                    final B2FileVersion file = new B2FileVersion(
                            "100", "test", 13, "text/plain", "",
                            Collections.emptyMap(), "", 0L);
                    return Collections.singletonList(file).iterator();
                };
            } else {
                return null;
            }
        });

        // Test open
        final B2FuseFilesystem fs = createFilesystem(b2);

        final StructFuseFileInfo info = createFileInfo("/test");
        int result = fs.open("/test", info);
        Assert.assertEquals(0, result);

        // Test read
        final ByteBuffer buffer = ByteBuffer.allocate(5);
        result = fs.read("/test", buffer, 5, 0, info);
        Assert.assertEquals(5, result);
        Assert.assertEquals("Hello", new String(buffer.array(), 0, 5));

        buffer.position(0);
        result = fs.read("/test", buffer, 5, 5, info);
        Assert.assertEquals(5, result);
        Assert.assertEquals(", wor", new String(buffer.array(), 0, 5));

        buffer.position(0);
        result = fs.read("/test", buffer, 3, 10, info);
        Assert.assertEquals(3, result);
        Assert.assertEquals("ld!", new String(buffer.array(), 0, 3));

        // Test release
        result = fs.release("/test", info);
        Assert.assertEquals(0, result);
    }

    /**
     * Creates a new B2 Storage Client.
     */
    private B2StorageClient createB2StorageClient() throws B2Exception {
        final B2StorageClient b2 = Mockito.mock(B2StorageClient.class);
        Mockito.when(b2.getBucketOrNullByName(BUCKET)).thenReturn(
                new B2Bucket("TEST", BUCKET, BUCKET, "", Collections.emptyMap(),
                             Collections.emptyList(), 0));
        Mockito.when(b2.unfinishedLargeFiles(BUCKET)).thenReturn(Collections::emptyIterator);
        return b2;
    }

    /**
     * Creates a new file with the specified path.
     */
    private StructFuseFileInfo createFileInfo(final String path) {
        final Pointer pointer = Pointer.wrap(Runtime.getSystemRuntime(), ByteBuffer.allocate(1024));
        try {
            final Constructor<StructFuseFileInfo> constructor =
                    StructFuseFileInfo.class.getDeclaredConstructor(Pointer.class, String.class);
            constructor.setAccessible(true);
            return constructor.newInstance(pointer, path);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new B2 Filey System.
     */
    private B2FuseFilesystem createFilesystem(final B2StorageClient b2Client) {
        final B2FuseFilesystem fs = new B2FuseFilesystem() {
            @Override
            B2StorageClient newS3() {
                return b2Client;
            }
        };
        return fs.init(BUCKET, Mockito.mock(B2ClientConfig.class), createFlags());
    }

    /**
     * Creates a new flag storage.
     */
    private FlagStorage createFlags() {
        final FlagStorage flags = new FlagStorage();
        flags.mountOptions = new HashMap<>();
        // File system
        flags.dirMode = 0755;
        flags.fileMode = 0644;
        flags.uid = 0;
        flags.gid = 0;
        // Tuning,
        flags.statCacheTtl = Duration.of(60, ChronoUnit.SECONDS);
        flags.TypeCacheTtl = Duration.of(60, ChronoUnit.SECONDS);

        flags.mountPointArg = MOUNT_POINT;
        flags.mountPoint = MOUNT_POINT;

        return flags;
    }
}
