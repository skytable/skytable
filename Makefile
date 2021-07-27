export ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
ADDITIONAL_SOFTWARE=
# (DEF) Either prepare --target triple-x-y OR have an empty value
TARGET_ARG =
ifneq ($(origin TARGET),undefined)
TARGET_ARG +=--target ${TARGET}
ifeq ($(TARGET),x86_64-unknown-linux-musl)
# we need musl-tools for 64-bit musl targets
ADDITIONAL_SOFTWARE += sudo apt-get update && sudo apt install musl-tools -y
endif
ifeq ($(TARGET),i686-unknown-linux-gnu)
# we need gcc-multilib on 32-bit linux targets
ADDITIONAL_SOFTWARE += sudo apt-get update && sudo apt install gcc-multilib -y
endif
endif

# (DEF) display a message if no additional packages are required for this target
ifeq ($(ADDITIONAL_SOFTWARE),)
ADDITIONAL_SOFTWARE += echo "info: No additional software required for this target"
endif

BUILD_VERBOSE = cargo build --verbose $(TARGET_ARG)

# (DEF) Create empty commands
STOP_SERVER =
BUILD_COMMAND =
BUILD_SERVER_COMMAND =
TEST_COMMAND =
RELEASE_COMMAND =
START_COMMAND =

# (DEF) Add cmd /c for windows due to OpenSSL issues or just add cargo run for non-windows
ifeq ($(OS),Windows_NT)
# export windows specifc rustflags
export RUSTFLAGS = -Ctarget-feature=+crt-static
# add command to use cmd because OpenSSL can't build on Windows bash
BUILD_COMMAND += cmd /C set RUSTFLAGS = -Ctarget-feature=+crt-static
# same thing here
BUILD_SERVER_COMMAND += cmd /C
TEST_COMMAND += cmd /C
STOP_SERVER += taskkill.exe /F /IM skyd.exe
RELEASE_COMMAND += cmd /C
START_COMMAND += cmd /C START /B
else
STOP_SERVER += pkill skyd
# make sure to set executable permissions
endif

# (DEF) Assemble the commands 
BUILD_SERVER_COMMAND += $(BUILD_VERBOSE)
BUILD_SERVER_COMMAND += -p skyd
RELEASE_SERVER_COMMAND =
RELEASE_SERVER_COMMAND += $(BUILD_SERVER_COMMAND)
RELEASE_SERVER_COMMAND += --release
RELEASE_COMMAND += cargo build --release $(TARGET_ARG)
BUILD_COMMAND += $(BUILD_VERBOSE)
TEST_COMMAND += cargo test $(TARGET_ARG)
START_COMMAND += cargo run $(TARGET_ARG) -p skyd
START_COMMAND_RELEASE =
START_COMMAND_RELEASE += ${START_COMMAND}
START_COMMAND_RELEASE += --release
START_COMMAND += -- --noart --nosave
START_COMMAND += --sslchain cert.pem --sslkey key.pem
START_COMMAND_RELEASE += -- --noart --nosave
ifneq ($(OS),Windows_NT)
START_COMMAND += &
START_COMMAND_RELEASE += &
endif
# (DEF) Prepare release bundle commands
BUNDLE=
ifeq ($(origin TARGET),undefined)
# no target defined. but check for windows
ifeq ($(OS),Windows_NT)
# windows, so we need exe
BUNDLE += cd target/release &&
BUNDLE += 7z a ../../../bundle.zip skysh.exe skyd.exe sky-bench.exe
else
# not windows, so no exe
BUNDLE+=zip -j bundle.zip target/release/skysh target/release/skyd target/release/sky-bench
endif
else
# target was defined, but check for windows
ifeq ($(OS),Windows_NT)
# windows, so we need exe
BUNDLE += cd target/${TARGET}/release &&
BUNDLE+=7z a ../../../sky-bundle-${VERSION}-${ARTIFACT}.zip skysh.exe skyd.exe sky-bench.exe
else
# not windows, so no exe
ifneq ($(origin CARGO_TARGET_DIR),undefined)
# target defined and target dir. use this instead of target/
BUNDLE+=zip -j sky-bundle-${VERSION}-${ARTIFACT}.zip ${CARGO_TARGET_DIR}/${TARGET}/release/skysh ${CARGO_TARGET_DIR}/${TARGET}/release/skyd ${CARGO_TARGET_DIR}/${TARGET}/release/sky-bench
else
# just the plain old target/${TARGET} path
BUNDLE+=zip -j sky-bundle-${VERSION}-${ARTIFACT}.zip target/${TARGET}/release/skysh target/${TARGET}/release/skyd target/${TARGET}/release/sky-bench
endif
endif
endif

.pre:
	@echo "===================================================================="
	@echo "Installing any additional dependencies"
	@echo "===================================================================="
	@$(ADDITIONAL_SOFTWARE)
build: .pre
	@echo "===================================================================="
	@echo "Building all binaries in debug mode (unoptimized)"
	@echo "===================================================================="
	@$(BUILD_COMMAND)
.build-server: .pre
	@echo "===================================================================="
	@echo "Building server binary in debug mode (unoptimized)"
	@echo "===================================================================="
	@$(BUILD_SERVER_COMMAND)
release: .pre
	@echo "===================================================================="
	@echo "Building all binaries in release mode (optimized)"
	@echo "===================================================================="
	cargo build --release --verbose $(TARGET_ARG)
.release-server:
	@echo "===================================================================="
	@echo "Building server binary in release mode (optimized)"
	@echo "===================================================================="
	@$(RELEASE_SERVER_COMMAND)
test: .build-server
	@echo "===================================================================="
	@echo "Starting database server in background"
	@echo "===================================================================="
	@chmod +x ci/ssl.sh && bash ci/ssl.sh
	@${START_COMMAND}
# sleep for 5s to let the server start up
	@sleep 5
	@echo "===================================================================="
	@echo "Running all tests"
	@echo "===================================================================="
	cargo test $(TARGET_ARG)
	@$(STOP_SERVER)
	@sleep 2
	@rm -f .sky_pid cert.pem key.pem
stress: .release-server
	@echo "===================================================================="
	@echo "Starting database server in background"
	@echo "===================================================================="
	@${START_COMMAND_RELEASE}
# sleep for 5s to let the server start up
	@sleep 5
	cargo run $(TARGET_ARG) --release -p stress-test
	@echo "===================================================================="
	@echo "Stress testing (all)"
	@echo "===================================================================="
	@$(STOP_SERVER)
	@rm -f .sky_pid cert.pem key.pem
bundle: release
	@echo "===================================================================="
	@echo "Creating bundle for platform"
	@echo "===================================================================="
	@$(BUNDLE)
clean:
	@echo "===================================================================="
	@echo "Cleaning up target folder"
	@echo "===================================================================="
	cargo clean
