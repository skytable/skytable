use cc;
fn main() {
    cc::Build::new()
    .file("native/fscposix.c")
    .compile("libflock.a")
}