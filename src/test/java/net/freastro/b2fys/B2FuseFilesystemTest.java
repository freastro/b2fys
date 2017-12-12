package net.freastro.b2fys;

import com.backblaze.b2.client.B2ClientConfig;
import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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
     * Test reading directory entries.
     */
    @Test
    public void testReadDir() throws Exception {
        final B2StorageClient b2 = Mockito.mock(B2StorageClient.class);
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
        Mockito.when(b2.unfinishedLargeFiles(BUCKET))
                .thenReturn(() -> Collections.<B2FileVersion>emptyList().iterator());

        final B2FuseFilesystem fs = createFilesystem(b2);

        final StructFuseFileInfo info = createFileInfo("/");
        int result = fs.opendir("/", info);
        Assert.assertEquals(0, result);

        final DirectoryFiller directoryFiller = Mockito.mock(DirectoryFiller.class);
        Mockito.when(directoryFiller.add(Mockito.any())).then(answer -> {
            final List<String> list = new ArrayList<>(answer.getArgument(0));
            Assert.assertEquals(3, list.size());
            return null;
        });
        result = fs.readdir("/", info, directoryFiller);
        Assert.assertEquals(0, result);
        Mockito.verify(directoryFiller, Mockito.times(1)).add(Mockito.any());

        result = fs.releasedir("/", info);
        Assert.assertEquals(0, result);
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
