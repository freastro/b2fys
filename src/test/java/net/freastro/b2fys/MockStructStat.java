package net.freastro.b2fys;

import java.nio.ByteBuffer;

import co.paralleluniverse.fuse.StructStat;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;

public class MockStructStat extends StructStat {

    private long atime;
    private long blksize;
    private long blocks;
    private long ctime;
    private long dev;
    private long gid;
    private long ino;
    private long mode;
    private long mtime;
    private long nlink;
    private final String path;
    private long rdev;
    private long size;
    private long uid;

    public MockStructStat(final String path) {
        super(Pointer.wrap(Runtime.getSystemRuntime(), ByteBuffer.allocate(1024)), path);
        this.path = path;
    }

    public String path() {
        return path;
    }

    public long atime() {
        return atime;
    }

    @Override
    public StructStat atime(long sec) {
        this.atime = sec;
        return this;
    }

    @Override
    public StructStat atime(long sec, long nsec) {
        this.atime = sec;
        return this;
    }

    public long blksize() {
        return blksize;
    }

    @Override
    public StructStat blksize(long blksize) {
        this.blksize = blksize;
        return this;
    }

    public long blocks() {
        return blocks;
    }

    @Override
    public StructStat blocks(long blocks) {
        this.blocks = blocks;
        return this;
    }

    public long ctime() {
        return ctime;
    }

    @Override
    public StructStat ctime(long sec) {
        this.ctime = sec;
        return this;
    }

    @Override
    public StructStat ctime(long sec, long nsec) {
        this.ctime = sec;
        return this;
    }

    public long dev() {
        return dev;
    }

    @Override
    public StructStat dev(long dev) {
        this.dev = dev;
        return this;
    }

    @Override
    public StructStat gen(long gen) {
        return this;
    }

    public long gid() {
        return gid;
    }

    @Override
    public StructStat gid(long gid) {
        this.gid = gid;
        return this;
    }

    public long ino() {
        return ino;
    }

    @Override
    public StructStat ino(long ino) {
        this.ino = ino;
        return this;
    }

    @Override
    public StructStat lspare(long lspare) {
        return this;
    }

    @Override
    public long mode() {
        return this.mode;
    }

    @Override
    public StructStat mode(long bits) {
        this.mode = bits;
        return this;
    }

    public long mtime() {
        return mtime;
    }

    @Override
    public StructStat mtime(long sec) {
        this.mtime = sec;
        return this;
    }

    @Override
    public StructStat mtime(long sec, long nsec) {
        this.mtime = sec;
        return this;
    }

    public long nlink() {
        return nlink;
    }

    @Override
    public StructStat nlink(long nlink) {
        this.nlink = nlink;
        return this;
    }

    @Override
    public StructStat qspare(long qspare) {
        return this;
    }

    public long rdev() {
        return rdev;
    }

    @Override
    public StructStat rdev(long rdev) {
        this.rdev = rdev;
        return this;
    }

    @Override
    public StructStat setAllTimes(long sec, long nsec) {
        atime = ctime = mtime = sec;
        return this;
    }

    @Override
    public StructStat setAllTimesMillis(long millis) {
        this.setAllTimesSec(millis / 1000);
        return this;
    }

    @Override
    public StructStat setAllTimesSec(long sec) {
        return setTimes(sec, 0, sec, 0, sec, 0, sec, 0);
    }

    @Override
    public StructStat setAllTimesSec(long atime, long mtime, long ctime) {
        return setTimes(atime, 0, mtime, 0, ctime, 0, 0, 0);
    }

    @Override
    public StructStat setAllTimesSec(long atime, long mtime, long ctime, long birthtime) {
        return setTimes(atime, 0, mtime, 0, ctime, 0, birthtime, 0);
    }

    @Override
    public StructStat setTimes(long atime_sec, long atime_nsec, long mtime_sec, long mtime_nsec,
                               long ctime_sec, long ctime_nsec) {
        return setTimes(atime_sec, atime_nsec, mtime_sec, mtime_nsec, ctime_sec, ctime_nsec, 0,
                        0);
    }

    @Override
    public StructStat setTimes(long atime_sec, long atime_nsec, long mtime_sec, long mtime_nsec,
                               long ctime_sec, long ctime_nsec, long birthtime_sec,
                               long birthtime_nsec) {
        this.atime = atime_sec;
        this.ctime = ctime_sec;
        this.mtime = mtime_sec;
        return this;
    }

    public long size() {
        return size;
    }

    @Override
    public StructStat size(long size) {
        this.size = size;
        return this;
    }

    public long uid() {
        return uid;
    }

    @Override
    public StructStat uid(long uid) {
        this.uid = uid;
        return this;
    }

    @Override
    public String toString() {
        return "MockStructStat";
    }
}
