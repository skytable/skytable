BUILD_HARNESS:=cargo build -p harness
RUN_HARNESS:= target/debug/harness
.harness:
	@${BUILD_HARNESS}
test: .harness
	@${RUN_HARNESS} test
bundle: .harness
	@${RUN_HARNESS} bundle
deb: .harness
	@${RUN_HARNESS} deb
