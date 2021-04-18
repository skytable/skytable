fn main() {
    #[cfg(unix)]
    {
        println!("cargo:rerun-if-changed=native/flock-posix.c");
        cc::Build::new()
            .file("native/flock-posix.c")
            .compile("libflock-posix.a");
    }
}
