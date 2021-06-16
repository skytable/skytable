ADDITIONAL_SOFTWARE=
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

# display a message if no additional packages are required for this target
ifeq ($(ADDITIONAL_SOFTWARE),)
ADDITIONAL_SOFTWARE += echo "info: No additional software required for this target"
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
START_COMMAND += cargo run $(TARGET_ARG) -p skyd -- --noart --nosave

ifneq ($(OS),Windows_NT)
START_COMMAND += &
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
test: .build-server
	@echo "===================================================================="
	@echo "Starting database server in background"
	@echo "===================================================================="
	@${START_COMMAND}
# sleep for 5s to let the server start up
	@sleep 5
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
