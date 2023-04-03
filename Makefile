CC =	   clang
LINK =	   $(CC)
COV =      llvm-cov
PROFDATA = llvm-profdata

INCS = -Isrc -Ilib/log -Ideps/klib -I/usr/include/luajit-2.1
WARNING_FLAGS = -Wall -Wno-unused-value -Wno-unused-function -Wno-nullability-completeness -Wno-expansion-to-defined -Werror=implicit-function-declaration -Werror=incompatible-pointer-types
COMMON_CFLAGS = $(INCS) -pipe $(WARNING_FLAGS)
COV_FLAGS = -fprofile-instr-generate -fcoverage-mapping

ATS_CFLAGS = -DNAL_LOG_ATS -O2 -fPIC $(COMMON_CFLAGS)

NGX_CFLAGS = -DNAL_LOG_NGX -O2 -fPIC $(COMMON_CFLAGS)

#TEST_LOG_FLAG = -DNAL_LOG_NOP
TEST_LOG_FLAG = -DNAL_LOG_STDERR -DDDEBUG

TEST_CFLAGS = $(TEST_LOG_FLAG) -DUNITY_INCLUDE_DOUBLE -O0 -g3 -Ideps/test/unity $(COV_FLAGS) $(COMMON_CFLAGS)

STDERR_CFLAGS = -DNAL_LOG_STDERR -DDDEBUG -O0 -g3 -fPIC $(COMMON_CFLAGS)

LDFLAGS = -llmdb

NAL_HEADERS = src/nal_lmdb.h

LOG_STDERR_HEADERS = lib/log/nal_log.h

LOG_ATS_HEADERS = lib/log/nal_log.h \
                  lib/log/tslog.h

LOG_NGX_HEADERS = lib/log/nal_log.h \
                  lib/log/ngx_array.h \
                  lib/log/ngx_atomic.h \
                  lib/log/ngx_auto_config.h \
                  lib/log/ngx_auto_headers.h \
                  lib/log/ngx_config.h \
                  lib/log/ngx_core.h \
                  lib/log/ngx_cycle.h \
                  lib/log/ngx_errno.h \
                  lib/log/ngx_linux_config.h \
                  lib/log/ngx_list.h \
                  lib/log/ngx_log.h \
                  lib/log/ngx_queue.h \
                  lib/log/ngx_rbtree.h \
                  lib/log/ngx_string.h

SRCS = src/nal_lmdb.c \

UNITY_DEPS = test/unity/unity.h \
             test/unity/unity_internals.h

NAL_ATS_OBJS = objs/ats/nal_lmdb.o \

NAL_NGX_OBJS = objs/ngx/nal_log_ngx.o \
               objs/ngx/nal_lmdb.o \

NAL_TEST_OBJS = objs/test/nal_log_stderr.o \
                objs/test/nal_lmdb.o \
				objs/test/unity.o \

NAL_STDERR_OBJS = objs/stderr/nal_log_stderr.o \
                  objs/stderr/nal_lmdb.o \

SHLIBS = objs/libnal_lmdb_ats.so \
         objs/libnal_lmdb_ngx.so

INSTALL_LUA_FILES = nal_lmdb_ats.lua \
                    nal_lmdb_ngx.lua \
                    nal_lmdb_setup.lua

TEST_DB_DIR = /tmp/test_lmdb

build: $(SHLIBS)

install: $(SHLIBS)
	sudo install $(SHLIBS) /usr/lib/x86_64-linux-gnu/
	sudo install $(INSTALL_LUA_FILES) /usr/local/share/lua/5.1/

example: objs/libnal_lmdb_stderr.so
	@mkdir -p $(TEST_DB_DIR)
	LD_LIBRARY_PATH=objs luajit nal_lmdb_stderr_ex.lua

test: objs/shdict_test
	LLVM_PROFILE_FILE=objs/shdict_test.profraw objs/shdict_test

cov: objs/shdict_test
	LLVM_PROFILE_FILE=objs/shdict_test.profraw objs/shdict_test
	$(PROFDATA) merge -sparse objs/shdict_test.profraw -o objs/shdict_test.profdata
	$(COV) show objs/shdict_test -instr-profile=objs/shdict_test.profdata $(SRCS)

objs/shdict_test: test/main.c $(NAL_TEST_OBJS)
	$(CC) -o $@ $(TEST_CFLAGS) $^

format:
	ls src/*.[ch] | xargs clang-format -i -style=file

# build SHLIBS

objs/libnal_lmdb_ats.so: $(NAL_ATS_OBJS)
	$(LINK) -o $@ $^ $(LDFLAGS) -shared

objs/libnal_lmdb_ngx.so: $(NAL_NGX_OBJS)
	$(LINK) -o $@ $^ $(LDFLAGS) -shared

objs/libnal_lmdb_stderr.so: $(NAL_STDERR_OBJS)
	$(LINK) -o $@ $^ $(LDFLAGS) -shared

# build NAL_ATS_OBJS

objs/ats/nal_lmdb.o: src/nal_lmdb.c $(NAL_HEADERS) $(LOG_ATS_HEADERS)
	@mkdir -p objs/ats
	$(CC) -c $(ATS_CFLAGS) -o $@ $<

# build NAL_NGX_OBJS

objs/ngx/nal_log_ngx.o: lib/log/nal_log_ngx.c $(LOG_NGX_HEADERS)
	@mkdir -p objs/ngx
	$(CC) -c $(NGX_CFLAGS) -o $@ $<

objs/ngx/nal_lmdb.o: src/nal_lmdb.c $(NAL_HEADERS) $(LOG_NGX_HEADERS)
	@mkdir -p objs/ngx
	$(CC) -c $(NGX_CFLAGS) -o $@ $<

# build NAL_TEST_OBJS

objs/test/nal_log_stderr.o: lib/log/nal_log_stderr.c $(LOG_STDERR_HEADERS)
	@mkdir -p objs/test
	$(CC) -c $(TEST_CFLAGS) -o $@ $<

objs/test/nal_lmdb.o: src/nal_lmdb.c $(NAL_HEADERS) $(LOG_STDERR_HEADERS)
	@mkdir -p objs/test
	$(CC) -c $(TEST_CFLAGS) -o $@ $<

objs/test/unity.o: test/unity/unity.c $(UNITY_DEPS)
	@mkdir -p objs/test
	$(CC) -c $(TEST_CFLAGS) -o $@ $<

# build NAL_STDERR_OBJS

objs/stderr/nal_log_stderr.o: lib/log/nal_log_stderr.c $(LOG_STDERR_HEADERS)
	@mkdir -p objs/stderr
	$(CC) -c $(TEST_CFLAGS) -o $@ $<

objs/stderr/nal_lmdb.o: src/nal_lmdb.c $(NAL_HEADERS) $(LOG_STDERR_HEADERS)
	@mkdir -p objs/stderr
	$(CC) -c $(TEST_CFLAGS) -o $@ $<

clean:
	@rm -rf objs core.* $(TEST_DB_DIR)
