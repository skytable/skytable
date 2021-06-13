TARGET_ARG =
ifneq ($(origin TARGET),undefined)
TARGET_ARG += --target ${TARGET}
endif

ifeq ($(OS),Windows_NT)
export RUSTFLAGS = -Ctarget-feature=+crt-static
endif

START_SERVER = 
ifeq ($(OS),Windows_NT)
START_SERVER += START /B target/${TARGET}/skyd.exe
else
START_SERVER += cargo run -p skyd -- --nosave --noart &
endif

STOP_SERVER =
ifeq ($(OS),Windows_NT)
STOP_SERVER += taskkill.exe /F /IM skyd.exe
else
STOP_SERVER += pkill skyd
endif

debug-full:
	cargo build --verbose
debug-server:
	cargo build --verbose -p skyd $(TARGET_ARG)
release-full:
	cargo build --release --verbose
test: debug-server
	$(START_SERVER)
	cargo test $(TARGET_ARG) -- --test-threads=1
	$(STOP_SERVER)
	rm .sky_pid
clean:
	cargo clean
