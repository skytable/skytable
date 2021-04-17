#[cfg(unix)]
use cc;
fn main() {
    #[cfg(unix)]
    {
        println!("cargo:rerun-if-changed=native/fscposix.c");
        cc::Build::new()
            .file("native/fscposix.c")
            .compile("libflock-posix.a")
    }
}
