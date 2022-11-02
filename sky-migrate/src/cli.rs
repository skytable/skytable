use clap::Parser;

const HELP_TEMPLATE: &str = r#"
{before-help}{name} {version}
{author-with-newline}{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}
"#;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, help_template = HELP_TEMPLATE, arg_required_else_help = true)]
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

#[cfg(test)]
mod tests {
    use crate::Cli;
    use clap::error::ErrorKind;
    use clap::Parser;

    #[test]
    fn test_mandatory_args_success() {
        let args = vec!["sky-migrate", "-n", "localhost:1234", "-p", "/tmp/skyd1"];
        let cli = Cli::parse_from(args.into_iter());
        assert_eq!(cli.new, "localhost:1234");
        assert_eq!(cli.prevdir, "/tmp/skyd1");
        assert!(!cli.serial);
    }

    #[test]
    fn test_serial_enabled_success() {
        let args = vec![
            "sky-migrate",
            "-n",
            "localhost:1234",
            "-p",
            "/tmp/skyd1",
            "-s",
        ];
        let cli = Cli::parse_from(args.into_iter());
        assert_eq!(cli.new, "localhost:1234");
        assert_eq!(cli.prevdir, "/tmp/skyd1");
        assert!(cli.serial);
    }

    #[test]
    fn test_host_port_missing_failure() {
        let args = vec!["sky-migrate", "-p", "/tmp/skyd1"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(
            cli_result.unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn test_prevdir_missing_failure() {
        let args = vec!["sky-migrate", "-n", "localhost:8083"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(
            cli_result.unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn test_display_help_when_all_args_missing() {
        let args = vec!["sky-migrate"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(
            cli_result.unwrap_err().kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }
}
