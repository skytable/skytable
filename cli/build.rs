fn main() -> std::io::Result<()> {
    libsky::build_scripts::format_help_txt("skysh", "help_text/help", Default::default())
}
