B2fys is a high-performance, POSIX-ish [Blackblaze B2](https://www.backblaze.com/b2/) file system written in Java.

[![Build Status](https://travis-ci.org/freastro/b2fys.svg?branch=master)](https://travis-ci.org/freastro/b2fys)

Overview
========

B2fys allow you to mount a B2 bucket as a filey system. Currently only reads are supported. See [Goofys](https://github.com/kahing/goofys) for more information on Filey Systems. B2fys is a port of Goofys to use the B2 Java SDK.

Installation
============

To build from source:

```bash
mvn package
```

Usage
=====

For normal use:

```bash
export ACCOUNT_ID="<b2-account-id>"
export APPLICATION_KEY="<b2-application-key>"
java -cp b2fs-1.0-SNAPSHOT.jar net.freastro.b2fys.App -o sync_read <bucket-name> <mount-path>
```

For debugging:

```bash
export ACCOUNT_ID="<b2-account-id>"
export APPLICATION_KEY="<b2-application-key>"
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -cp .:b2fs-1.0-SNAPSHOT.jar net.freastro.b2fys.App --debug_fuse -o sync_read <bucket-name> <mount-path>
```

Sample logging configuration for debugging. Create a `log4j2.xml` file in the same directory as the jar:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="debug" name="MyApp" packages="">
  <Appenders>
    <Console name="STDOUT" target="SYSTEM_OUT">
      <PatternLayout pattern="%m%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Root level="DEBUG">
      <AppenderRef ref="STDOUT"/>
    </Root>
  </Loggers>
</Configuration>
```

License
=======

Copyright (C) 2015-2017 Ka-Hing Cheung
Copyright (C) 2017-2018 Gregory Hart

Licensed under the Apache License, Version 2.0

Current Status
==============

b2fys has been tested under Linux.

List of non-POSIX behaviours/limitations:

- read-only
- does not store file mode/owner/group
  - use `--(dir|file)-mode` or `--(uid|gid)` options
- does not support symlink or hardlink
- `ctime`, `atime` are always the same as `mtime`
