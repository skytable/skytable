TARGET_ARG =
ifneq ($(origin TARGET),undefined)
TARGET_ARG +=--target ${TARGET}
endif

CHMOD =
ifneq ($(OS),Windows_NT)
CHMOD += chmod +x
# add the path
ifeq ($(origin TARGET),undefined)
CHMOD += target/debug/skyd
else
CHMOD += target/${TARGET}/debug/skyd
endif
endif

BUILD_VERBOSE = cargo build --verbose $(TARGET_ARG)

# Create empty commands
STOP_SERVER =
BUILD_COMMAND =
BUILD_SERVER_COMMAND =
TEST_COMMAND =
RELEASE_COMMAND =
START_COMMAND =

ifeq ($(OS),Windows_NT)
# export windows specifc rustflags
export RUSTFLAGS = -Ctarget-feature=+crt-static
# add command to use cmd because OpenSSL can't build on Windows bash
BUILD_COMMAND += cmd /C set RUSTFLAGS = -Ctarget-feature=+crt-static
# same thing here
BUILD_SERVER_COMMAND += cmd /C
TEST_COMMAND += cmd /C
STOP_SERVER += taskkill.exe /F /IM skyd.exe
START_COMMAND += skyd.exe
RELEASE_COMMAND += cmd /C
START_COMMAND += cmd /C START /B
else
STOP_SERVER += pkill skyd
# make sure to set executable permissions
endif

# Assemble the commands 
BUILD_SERVER_COMMAND += $(BUILD_VERBOSE)
BUILD_SERVER_COMMAND += -p skyd
RELEASE_COMMAND += cargo build --release $(TARGET_ARG)
BUILD_COMMAND += $(BUILD_VERBOSE)
TEST_COMMAND += cargo test $(TARGET_ARG) -- --test-threads=1
START_COMMAND += cargo run -p skyd -- --noart --nosave

ifneq ($(OS),Windows_NT)
START_COMMAND += &
endif

build:
	@echo "===================================================================="
	@echo "Building all binaries in debug mode (unoptimized)"
	@echo "===================================================================="
	@$(BUILD_COMMAND)
build-server:
	@echo "===================================================================="
	@echo "Building server binary in debug mode (unoptimized)"
	@echo "===================================================================="
	@$(BUILD_SERVER_COMMAND)
release:
	@echo "===================================================================="
	@echo "Building all binaries in release mode (optimized)"
	@echo "===================================================================="
	cargo build --release --verbose $(TARGET_ARG)
test: build-server
	@echo "===================================================================="
	@echo "Starting database server in background"
	@echo "===================================================================="
	@${CHMOD}
	@${START_COMMAND}
	@echo "===================================================================="
	@echo "Running all tests"
	@echo "===================================================================="
	cargo test $(TARGET_ARG) -- --test-threads=1
	@$(STOP_SERVER)
	rm .sky_pid
clean:
	@echo "===================================================================="
	@echo "Cleaning up target folder"
	@echo "===================================================================="
	cargo clean
