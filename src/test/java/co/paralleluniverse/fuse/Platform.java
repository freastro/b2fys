package co.paralleluniverse.fuse;

final class Platform {

    public static final Platform.PlatformEnum platform() {
        return PlatformEnum.LINUX_X86_64;
    }

    public enum PlatformEnum {
        LINUX_X86_64,
        LINUX_I686,
        LINUX_PPC,
        MAC,
        MAC_MACFUSE,
        FREEBSD,
        LINUX_ARM
    }
}
