#[cfg(unix)]
use cc;
fn main() {
    #[cfg(unix)]
    cc::Build::new()
        .file("native/fscposix.c")
        .compile("libflock-posix.a")
}
