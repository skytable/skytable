BUILD_HARNESS:=cargo build -p harness
RUN_HARNESS:= target/debug/harness
.harness:
	@${BUILD_HARNESS}
test: .harness
	@${RUN_HARNESS} test
bundle: .harness
	@${RUN_HARNESS} bundle
deb:
	@${RUN_HARNESS} deb
