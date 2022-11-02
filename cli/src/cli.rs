use clap::{ArgAction, Parser};

const HELP_TEMPLATE: &str = r#"
{before-help}{name} {version}
{author-with-newline}{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}
"#;

#[derive(Parser, Debug)]
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

#[cfg(test)]
mod tests {
    use crate::cli::Cli;
    use clap::error::ErrorKind;
    use clap::Parser;

    #[test]
    fn test_no_user_args_picks_default_values() {
        let args = vec!["skysh"];
        let cli: Cli = Cli::parse_from(args.into_iter());
        assert_eq!(cli.host, "127.0.0.1");
        assert_eq!(cli.port, 2003);
        assert_eq!(cli.expressions, None);
        assert_eq!(cli.ssl_cert, None);
    }

    #[test]
    fn test_invalid_arg_fails_validation() {
        let args = vec!["skysh", "-p", "asd"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(cli_result.unwrap_err().kind(), ErrorKind::ValueValidation);

        let args = vec!["skysh", "-h", "-1"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(cli_result.unwrap_err().kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_arg_override_works_as_expected() {
        let args = vec![
            "skysh",
            "-p",
            "666",
            "-h",
            "devil",
            "--sslcert",
            "/tmp/mycert.pem",
            "-e",
            "SET X 100",
            "GET X",
            "UPDATE X 42",
        ];
        let cli: Cli = Cli::parse_from(args.into_iter());

        assert_eq!(cli.host, "devil");
        assert_eq!(cli.port, 666);
        assert_eq!(cli.ssl_cert, Some("/tmp/mycert.pem".into()));
        assert_eq!(
            cli.expressions,
            Some(vec![
                "SET X 100".into(),
                "GET X".into(),
                "UPDATE X 42".into()
            ])
        )
    }

}
