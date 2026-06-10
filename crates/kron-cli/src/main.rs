mod commands;

use std::env;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kron", about = "application time engine")]
struct Cli {
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Job {
        #[command(subcommand)]
        command: commands::job::JobCommand,
    },
    Log {
        #[command(subcommand)]
        command: commands::log::LogCommand,
    },
    Runtime {
        #[command(subcommand)]
        command: commands::runtime::RuntimeCommand,
    },
    Server {
        #[command(subcommand)]
        command: commands::server::ServerCommand,
    },
    Doctor,
}

fn main() {
    let cli = Cli::parse();
    let data_dir = cli
        .data_dir
        .or_else(|| env::var("KRON_HOME").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(".kron"));

    let result = match cli.command {
        Command::Job { command } => commands::job::run(command, &data_dir),
        Command::Log { command } => commands::log::run(command, &data_dir),
        Command::Runtime { command } => commands::runtime::run(command, &data_dir),
        Command::Server { command } => commands::server::run(command, &data_dir),
        Command::Doctor => commands::doctor::run(&data_dir),
    };

    if let Err(err) = result {
        eprintln!("kron: {err}");
        std::process::exit(1);
    }
}
