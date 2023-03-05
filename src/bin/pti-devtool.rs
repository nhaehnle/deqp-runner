use clap::{Parser, Subcommand};
use serde::Deserialize;

use deqp_runner::pti::{self, *};
use rand::prelude::*;
use utils::Result;

#[derive(Debug, Clone, Subcommand)]
enum Action {
    DevSample,
    DevTryRun,
}

#[derive(Debug, Parser)]
struct Cli {
    /// Path to the configuration file.
    #[arg(long)]
    config: std::path::PathBuf,

    /// More detailled logging.
    #[arg(long)]
    verbose: bool,

    /// Keep temporary files.
    #[arg(long)]
    keep_temps: bool,

    /// Seed for the random number generator. Default behavior is to use the
    /// system RNG to obtain a different seed on each run.
    #[arg(long)]
    seed: Option<u64>,

    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Deserialize)]
struct Config {
    deqp_vk: std::path::PathBuf,
    deqp_cases: Option<std::path::PathBuf>,
}

fn do_main() -> Result<()> {
    let args = Cli::parse();

    println!("{:?}", &args);

    let config = utils::read_bytes(args.config)?;
    let config: Config = serde_yaml::from_slice(&config)?;

    println!("{:?}", &config);

    let mut rng = args.seed.map(rand::rngs::StdRng::seed_from_u64).unwrap_or_else(rand::rngs::StdRng::from_entropy);

    let vulkan_cts_config = pti::vulkancts::Config {
        deqp_vk: config.deqp_vk,
        deqp_cases: config.deqp_cases,
        options: pti::vulkancts::Options {
            keep_temps: args.keep_temps,
            verbose: args.verbose,
            ..Default::default()
        }
    };
    let suite = pti::vulkancts::get_caselist(&vulkan_cts_config)?;
    let mut sampler = pti::suite::Sampler::new(&suite)?;

    match args.action {
    Action::DevSample => {
        for _ in 0..20 {
            let test = sampler.sample(&suite, &mut rng);
            println!("{}", suite.get_name(test));
        }
    },
    Action::DevTryRun => {
        let tests: Vec<_> = std::iter::repeat_with(|| sampler.sample(&suite, &mut rng)).take(20).collect();
        vulkancts::run_tests(&vulkan_cts_config, &suite, &tests)?;
    },
    }

    Ok(())
}

fn main() {
    if let Err(err) = do_main() {
        println!("{}", err);
        std::process::exit(1);
    }
}
