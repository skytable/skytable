# although this is exported by cargo, we'll export it again to use it in the Makefile
export ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
export MSYS_NO_PATHCONV:=1
SKYTEST_S1_FILE := $(ROOT_DIR)/ci/server1.toml
SKYTEST_S2_FILE := $(ROOT_DIR)/ci/server2.toml
# no additional software note
NO_ADDITIONAL_SOFTWARE := echo "No additional software required for this target"
# target argument
TARGET_ARG :=
# target folder path
TARGET_FOLDER := target/
# additional software installation
ADDITIONAL_SOFTWARE := 
# update variables depending on target
ifneq ($(origin TARGET),undefined)
  ifeq ($(TARGET),x86_64-unknown-linux-musl)
    # for MUSL builds, we need to install musl-tools
    ADDITIONAL_SOFTWARE += sudo apt-get update && sudo apt install musl-tools -y
  else ifeq ($(TARGET),i686-unknown-linux-gnu)
    # for 32-bit we need multilib
    ADDITIONAL_SOFTWARE += sudo apt-get update && sudo apt install gcc-multilib -y
  else
    ADDITIONAL_SOFTWARE += ${NO_ADDITIONAL_SOFTWARE}
  endif
TARGET_ARG += --target ${TARGET}
TARGET_FOLDER := $(addsuffix ${TARGET}/,${TARGET_FOLDER})
else
ADDITIONAL_SOFTWARE += ${NO_ADDITIONAL_SOFTWARE}
endif

TARGET_FOLDER := $(addsuffix release/,${TARGET_FOLDER})
# cargo build
CBUILD := cargo build $(TARGET_ARG)
# cargo test
CTEST := cargo test $(TARGET_ARG)
# cargo run
CRUN := cargo run $(TARGET_ARG)

# binary file paths
BINARY_SKYSH := $(TARGET_FOLDER)skysh
BINARY_SKYD := $(TARGET_FOLDER)skyd
BINARY_SKYBENCH := $(TARGET_FOLDER)sky-bench
BINARY_SKYMIGRATE := $(TARGET_FOLDER)sky-migrate
# archive command
ARCHIVE :=
# start background server command
START_SERVER := $(CRUN) -p skyd -- --withconfig "$(SKYTEST_S1_FILE)"
STOP_SERVER :=

ifeq ($(OS),Windows_NT)
  # on windows, so we need exe
  ARCHIVE += 7z a ourbundle.zip $(BINARY_SKYSH).exe $(BINARY_SKYD).exe $(BINARY_SKYBENCH).exe $(BINARY_SKYMIGRATE).exe
  # also add RUSTFLAGS
  export RUSTFLAGS = -Ctarget-feature=+crt-static
  # now add start command
  START_SERVER := cmd /C START /B $(START_SERVER) 
  # windows is funky with OpenSSL, so add these
  CBUILD := cmd /C $(CBUILD)
  CTEST := cmd /C $(CTEST)
  # finally add stop command
  STOP_SERVER := taskkill.exe /F /IM skyd.exe
else
  # not windows, so archive is easy
  ARCHIVE += zip -j ourbundle.zip $(BINARY_SKYSH) $(BINARY_SKYD) $(BINARY_SKYBENCH) $(BINARY_SKYMIGRATE)
  # now add start command
  START_SERVER := $(START_SERVER) &
  # add stop command
  STOP_SERVER := pkill skyd
endif

START_SERVER2 := $(subst $(SKYTEST_S1_FILE),$(SKYTEST_S2_FILE),$(START_SERVER))

# update the archive command if we have a version and artifact name
RENAME_ARTIFACT :=
ifneq ($(origin ARTIFACT),undefined)
  # so we have an artifact name
  ifneq ($(origin VERSION),undefined)
    # we also have the version name
	RENAME_ARTIFACT := sky-bundle-${VERSION}-${ARTIFACT}.zip
  else
    # no version name
	RENAME_ARTIFACT := sky-bundle-${ARTIFACT}.zip
  endif
else
  # no artifact (hack)
  RENAME_ARTIFACT := bundle.zip
endif

RENAME_ARTIFACT := $(addprefix mv ourbundle.zip ,${RENAME_ARTIFACT})

# cargo build (debug)
DEBUG := $(CBUILD)
# cargo test
TEST := $(CTEST)
# cargo build (release)
RELEASE := $(CBUILD) --release
# cargo build (release) for skyd,skysh,sky-migrate and sky-bench
RELEASE_BUNDLE := $(RELEASE) -p skyd -p sky-bench -p skysh -p sky-migrate
SEP=echo "============================================================"

.pre:
	@${SEP}
	@echo "Installing additional dependencies ..."
	@${ADDITIONAL_SOFTWARE}
	@${SEP}
build: .pre
	@${SEP}
	@echo "Building all binaries (debug) ..."
	@${DEBUG}
	@${SEP}
release: .pre
	@${SEP}
	@echo "Building all binaries (release) ..."
	@${RELEASE}
	@${SEP}
release-bundle: .pre
	@${SEP}
	@echo "Building binaries for packaging (release) ..."
	@${RELEASE_BUNDLE}
	@${SEP}
bundle: release-bundle
	@${SEP}
	@echo "Building and packaging bundle (release) ..."
	@${ARCHIVE}
	@${RENAME_ARTIFACT}
	@${SEP}
test: .pre
	@${SEP}
	@echo "Building and starting server in debug mode ..."
	@chmod +x ci/ssl.sh && bash ci/ssl.sh
	@${CBUILD} -p skyd
	@mkdir -p server1 && cd server1 && ${START_SERVER}
	@mkdir -p server2 && cd server2 && ${START_SERVER2}
	@echo "Sleeping for 10 seconds to let the server start up ..."
	@sleep 10
	@echo "Finished sleeping"
	@${SEP}
	@${SEP}
	@echo "Running all tests ..."
	@${TEST}
	@echo "Waiting for server to shut down ..."
	@${STOP_SERVER}
	@sleep 10
	@echo "Removing temporary files ..."
	@rm -rf .sky_pid server1 server2 .skytest_*
	@${SEP}
clean:
	@${SEP}
	@echo "Cleaning up target folder ..."
	cargo clean
	@${SEP}
testclean:
	@rm -rf server1 server2
deb: release-bundle
	@${SEP}
	@echo "Making a debian package (release) ..."
	@echo "Installing tools for packaging ..."
	@cargo install cargo-deb
	@echo "Packaging ..."
	@cargo deb $(TARGET_ARG) --no-build --manifest-path=server/Cargo.toml --output skytable-${VERSION}-${ARTIFACT}.deb
	@${SEP}
checkcmd:
	@echo $(START_SERVER)
	@echo $(START_SERVER2)
