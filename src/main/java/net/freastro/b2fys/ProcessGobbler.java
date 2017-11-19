package net.freastro.b2fys;

/*
Copyright (c) 2015 Parallel Universe
Copyright (c) 2012-2015 Etienne Perot
Copyright (c) 2015 Sergey Tselovalnikov

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED THE COPYRIGHT HOLDERS AND CONTRIBUTORS ''AS IS'' AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Run a process, grab all from stdout, return that.
 */
final class ProcessGobbler {

    private static final class Gobbler extends Thread {

        private final InputStream stream;
        private String contents = null;
        private boolean failed = false;

        Gobbler(InputStream stream) {
            this.stream = stream;
            start();
        }

        private String getContents() {
            if (failed) {
                return null;
            }

            return contents;
        }

        @Override
        public final void run() {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            final StringBuilder contents = new StringBuilder();
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    contents.append(line);
                }
            } catch (IOException e) {
                failed = true;
                return;
            }
            this.contents = contents.toString();
        }
    }

    private final Process process;
    private final Gobbler stdout;
    private final Gobbler stderr;
    private Integer returnCode = null;

    ProcessGobbler(String... args) throws IOException {
        process = new ProcessBuilder(args).start();
        stdout = new Gobbler(process.getInputStream());
        stderr = new Gobbler(process.getErrorStream());
    }

    int getReturnCode() {
        try {
            returnCode = process.waitFor();
        } catch (InterruptedException e) {
            // Too bad
        }
        return returnCode;
    }

    String getStderr() {
        try {
            stderr.join();
        } catch (InterruptedException e) {
            return null;
        }
        return stderr.getContents();
    }

    String getStdout() {
        try {
            stdout.join();
        } catch (InterruptedException e) {
            return null;
        }
        return stdout.getContents();
    }
}
