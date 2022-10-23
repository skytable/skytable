use clap::Parser;

const HELP_TEMPLATE: &'static str = r#"
{before-help}{name} {version}
{author-with-newline}{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}
"#;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None, help_template=HELP_TEMPLATE)]
pub struct Cli {
    #[arg(
        short = 'n',
        long = "new",
        help = "The <host>:<port> combo for the new instance",
        value_name = "HOST:PORT"
    )]
    pub new: String,

    #[arg(
        short = 'p',
        long = "prevdir",
        help = "Path to the previous installation location",
        value_name = "PREVDIR"
    )]
    pub prevdir: String,

    #[arg(
        short = 's',
        long,
        help = "Transfer entries one-by-one instead of all at once to save memory"
    )]
    pub serial: bool,
}
