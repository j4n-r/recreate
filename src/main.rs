use anyhow::Result;
use clap::{Parser, Subcommand};
use lsm::DB;
#[derive(Debug, Parser)]
#[command(name = "lsm")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    let mut db = DB::create("wal.log".to_owned())?;
    db.fill()?;

    match cli.command {
        Commands::Put { key, value } => {
            db.put(key.as_str(), value.as_str())?;
            Ok(())
        },
        Commands::Get { key } => {
            if let Some(value) = db.get(key.as_str())? {
                println!("{key}: {}", String::from_utf8(value.into())?);
            } else {
                println!("Not found");
            }
            Ok(())
        },
        Commands::Delete { key } => {
            db.delete(key.as_str())?;
            Ok(())
        },
    }
}
