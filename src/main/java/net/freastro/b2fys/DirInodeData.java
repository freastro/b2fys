package net.freastro.b2fys;

import java.time.Instant;

class DirInodeData {

    // these 2 refer to readdir of the children
    String lastOpenDir;
    int lastOpenDirIdx;
    byte seqOpenDirScore;
    Instant dirTime;

    Inode[] children;
}
