TARGET_ARG =
ifneq ($(origin TARGET),undefined)
TARGET_ARG += --target ${TARGET}
endif

ifeq ($(OS),Windows_NT)
export RUSTFLAGS = -Ctarget-feature=+crt-static
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
	cargo run -p skyd -- --nosave --noart &
	cargo test $(TARGET_ARG) -- --test-threads=1
	$(STOP_SERVER)
	rm .sky_pid
clean:
	cargo clean
