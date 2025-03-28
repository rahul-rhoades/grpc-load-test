use clap::Parser;
use futures::StreamExt;
use std::error::Error;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use chrono::Local;
use yellowstone_grpc_client::{
    GeyserGrpcClient, GeyserGrpcClientError};

use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
                                     SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

/// Command line arguments
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Yellowstone gRPC endpoint
    #[clap(long, default_value = "https://api.mainnet-beta.solana.com:443")]
    endpoint: String,

    /// X-Token for authentication (if required)
    #[clap(long)]
    token: Option<String>,

    /// Account address to subscribe to
    #[clap(long)]
    account: Option<String>,

    /// Subscribe to blocks
    #[clap(long)]
    blocks: bool,

    /// Subscribe to transactions
    #[clap(long)]
    transactions: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args = Args::parse();

    // Configure connection client
    let mut client = yellowstone_grpc_client::GeyserGrpcClient::build_from_shared("https://obsidian-matrix.fleet.hellomoon.io:50051".to_string())
        .unwrap()
        .x_token(Some("HMG-QAHP-JNY7-WJN3-PUCN".to_string()))
        .unwrap()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(3))
        .keep_alive_while_idle(true)
        .connect()
        .await
        .unwrap();

    // Create the gRPC client
    let (mut client_result, mut subscription_result) = client
        .subscribe_with_request(Some(create_subscription_request(&args)))
        .await.unwrap();

    println!("Connected to Yellowstone gRPC at {}", args.endpoint);

    // Spawn a task to process the subscription stream
    while let Some(msg) = subscription_result.next().await {
        if let Ok(msg) = msg {
            if let Some(update_one_of) = msg.update_oneof {
                match update_one_of {
                    UpdateOneof::Account(_) => {}
                    UpdateOneof::Slot(data) => {
                        let st = SystemTime::now();
                        println!(
                            "Slot update {:?} time {}",
                             data.slot,
                             Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                        );
                    }
                    UpdateOneof::Transaction(_) => {}
                    UpdateOneof::TransactionStatus(_) => {}
                    UpdateOneof::Block(_) => {}
                    UpdateOneof::Ping(_) => {}
                    UpdateOneof::Pong(_) => {}
                    UpdateOneof::BlockMeta(_) => {}
                    UpdateOneof::Entry(_) => {}
                }
            }
        }
    }
    Ok(())
}

/// Create subscription request based on command-line arguments
fn create_subscription_request(args: &Args) -> SubscribeRequest {
    let mut request = SubscribeRequest::default();

    // Add accounts filter if provided
    if let Some(_account) = &args.account {
        request.accounts.insert(
            "accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
            }
        );
    }

    // Add blocks filter if requested
    if args.blocks {
        request.blocks.insert(
            "blocks".to_string(),
            SubscribeRequestFilterBlocks {
                ..SubscribeRequestFilterBlocks::default()
            }
        );

        // Also subscribe to slots
        request.slots.insert(
            "slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: None,
            }
        );
    }

    // Add transactions filter if requested
    if args.transactions {
        request.transactions.insert(
            "transactions".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            }
        );
    }

    request
}