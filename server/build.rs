fn main() -> std::io::Result<()> {
    libsky::build_scripts::format_help_txt("skyd", "help_text/help", Default::default())
}
