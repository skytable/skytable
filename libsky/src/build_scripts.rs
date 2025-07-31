use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{self, Write},
    path::Path,
};

pub fn format_help_txt(
    binary_name: &str,
    help_text_path: impl AsRef<Path>,
    arguments: HashMap<&str, &str>,
) -> io::Result<()> {
    fmt_help_text(binary_name, &arguments, help_text_path)
}

fn fmt_help_text<'a>(
    binary_name: &str,
    arguments: &HashMap<&str, &str>,
    help_text_path: impl AsRef<Path>,
) -> Result<(), io::Error> {
    let help_msg = fs::read_to_string(help_text_path)?;
    let content = super::utils::format(&help_msg, arguments, true);
    // write
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join(binary_name);
    let mut f = File::create(dest_path)?;
    f.write_all(content.as_bytes())?;
    Ok(())
}

pub fn format_all_help_txt(
    binary_name: &str,
    directory: &str,
    arguments: HashMap<&str, &str>,
) -> io::Result<()> {
    let dir = fs::read_dir(directory)?;
    for item in dir {
        let item = item?;
        let item_path = item.path();
        let file_name = &item.file_name();
        let item_name = file_name.to_str().unwrap();
        fmt_help_text(&format!("{binary_name}-{item_name}"), &arguments, item_path)?;
    }
    Ok(())
}
