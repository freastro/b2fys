package net.freastro.b2fys;

// A struct representing an entry within a directory file, describing a child.
public class Dirent {

    // The (opaque) offset within the directory file of the entry following this
    // one.
    long offset;

    // The inode of the child file or directory, and its name within the parent.
    long inode;
    String name;

    // The type of the child. The zero value (DT_Unknown) is legal, but means
    // that the kernel will need to call GetATtr when the type is needed.
    int type;
}
