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

    #[arg(
        short = 'c',
        long = "connections",
        help = "Sets the number of simultaneous clients",
        value_name = "COUNT",
        default_value_t = 10
    )]
    pub connections: usize,

    #[arg(
        short = 'r',
        long = "runs",
        help = "Sets the number of times the entire test should be run",
        value_name = "RUNS",
        default_value_t = 5
    )]
    pub runs: usize,

    #[arg(
        short = 's',
        long = "kvsize",
        help = "Sets the size of the key/value pairs",
        value_name = "BYTES",
        default_value_t = 3
    )]
    pub kvsize: usize,

    #[arg(
        short = 'q',
        long = "queries",
        help = "Sets the number of queries to run",
        value_name = "QUERIES",
        default_value_t = 100_000
    )]
    pub query_count: usize,

    #[arg(
        short = 'j',
        long = "json",
        help = "Sets output type to JSON",
        default_value_t = false
    )]
    pub json: bool,

    #[arg(long, help="Print help information", action=ArgAction::Help)]
    pub help: Option<bool>,
}

#[cfg(test)]
mod tests {

    use crate::Cli;
    use clap::error::ErrorKind;
    use clap::Parser;

    #[test]
    fn test_no_user_args_picks_default_values() {
        let args = vec!["sky-bench"];
        let cli = Cli::parse_from(args.into_iter());
        assert_eq!(cli.host, "127.0.0.1");
        assert_eq!(cli.port, 2003);
        assert_eq!(cli.connections, 10);
        assert_eq!(cli.runs, 5);
        assert_eq!(cli.kvsize, 3);
        assert_eq!(cli.query_count, 100_000);
        assert!(!cli.json);
    }

    #[test]
    fn test_invalid_arg_fails_validation() {
        let args = vec!["sky-bench", "-p", "asd"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(cli_result.unwrap_err().kind(), ErrorKind::ValueValidation);

        let args = vec!["sky-bench", "-c", "-1"];
        let cli_result: Result<Cli, clap::Error> = Cli::try_parse_from(args.into_iter());

        assert!(cli_result.is_err());
        assert_eq!(cli_result.unwrap_err().kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_arg_override_works_as_expected() {
        let args = vec!["sky-bench", "-p", "666", "-h", "devil", "--json"];
        let cli: Cli = Cli::parse_from(args.into_iter());

        assert_eq!(cli.host, "devil");
        assert_eq!(cli.port, 666);
        assert!(cli.json);
    }
}
