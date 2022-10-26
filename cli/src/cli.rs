use clap::{ArgAction, Parser};

const HELP_TEMPLATE: &'static str = r#"
{before-help}{name} {version}
{author-with-newline}{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}
"#;

#[derive(Parser)]
#[command(author, version, about, long_about=None, disable_help_flag=true, help_template=HELP_TEMPLATE)]
pub struct Cli {
    #[arg(
        short = 'C',
        long = "sslcert",
        help = "Sets the PEM certificate to use for SSL connections",
        value_name = "CERT"
    )]
    pub ssl_cert: Option<String>,

    #[arg(short = 'e', long = "eval", help = "Run one or more expressions without REPL", value_name = "EXPRESSION", num_args=0..)]
    pub expressions: Option<Vec<String>>,

    #[arg(
        short,
        long,
        help = "Sets the remote host to connect to",
        default_value = "127.0.0.1",
        value_name = "HOST"
    )]
    pub host: String,

    #[arg(
        short,
        long,
        help = "Sets the remote port to connect to",
        default_value_t = 2003,
        value_name = "PORT"
    )]
    pub port: u16,

    #[arg(long, help="Print help information", action=ArgAction::Help)]
    pub help: Option<bool>,
}
