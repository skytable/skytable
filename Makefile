BUILD_HARNESS:=cargo build -p harness
RUN_HARNESS:= target/debug/harness
.harness:
	@${BUILD_HARNESS}
build:
	@cargo build
test: .harness
	@${RUN_HARNESS} test
bundle: .harness
	@${RUN_HARNESS} bundle
bundle-dbg: .harness
	@${RUN_HARNESS} bundle-dbg
deb: .harness
	@${RUN_HARNESS} deb
