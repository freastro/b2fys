package net.freastro.b2fys;

// InodeAttributes contains attributes for a file or directory inode. It
// corresponds to struct inode (cf. http://goo.gl/tvYyQt).
public class FuseInodeAttributes {

    long size;

    // The number of incoming hard links to this inode.
    int nlink;

    // The mode of the inode. This is exposed to the user in e.g. the result of
    // fstat(2).
    //
    // Note that in contrast to the defaults for FUSE, this package mounts file
    // systems in a manner such that the kernel checks inode permissions in the
    // standard posix way. This is implemented by setting the default_permissions
    // mount option (cf. http://goo.gl/1LxOop and http://goo.gl/1pTjuk).
    //
    // For example, in the case of mkdir:
    //
    //  *  (http://goo.gl/JkdxDI) sys_mkdirat calls inode_permission.
    //
    //  *  (...) inode_permission eventually calls do_inode_permission.
    //
    //  *  (http://goo.gl/aGCsmZ) calls i_op->permission, which is
    //     fuse_permission (cf. http://goo.gl/VZ9beH).
    //
    //  *  (http://goo.gl/5kqUKO) fuse_permission doesn't do anything at all for
    //     several code paths if FUSE_DEFAULT_PERMISSIONS is unset. In contrast,
    //     if that flag *is* set, then it calls generic_permission.
    //
    long mode;

    // Time information. See `man 2 stat` for full details.
    long atime; // Time of last access
    long mtime; // Time of last modification
    long ctime; // Time of last modification to inode
    long crtime; // Time of creation (OS X only)

    // Ownership information
    long uid;
    long gid;
}
