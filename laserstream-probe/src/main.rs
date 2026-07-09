// LaserStream latency probe — watches the stacSOL arb pool reserve vaults and logs
// each gRPC account-update PUSH with a high-precision local timestamp + slot.
// Also subscribes to SLOT ticks as a connectivity heartbeat (throttled).
use futures_util::StreamExt;
use helius_laserstream::grpc::subscribe_update::UpdateOneof;
use helius_laserstream::{grpc::*, subscribe, LaserstreamConfig};
use std::collections::HashMap;

const VAULTS: &[(&str, &str)] = &[
    ("DK4rgbFv5f4PSZTVmJSqTDL55WwwEjHa9m6xUeh3nRo3", "A:staccana-vault(PumpSwap)"),
    ("FmyQzcoKX6eZq5s7DuNGX3zpJ27WMjL52JjTC4Gq4yhq", "A:WSOL-vault(PumpSwap)"),
    ("4NboZmfYYJhZkYMc47Quims8TyGLyhTBygA6KrNcrHHW", "B:staccana-vault(RaydiumCPMM)"),
    ("84FmG9UDjgFS55XnoP3AdZVVW8ukZJZ7jzWDJDd9A5Pv", "B:stacSOL/WSOL-vault(RaydiumCPMM)"),
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    let endpoint = std::env::var("LASERSTREAM_ENDPOINT")
        .unwrap_or_else(|_| "https://laserstream-mainnet-ewr.helius-rpc.com".to_string());
    let api_key = std::env::var("LASERSTREAM_API_KEY")
        .expect("set LASERSTREAM_API_KEY (from your Helius LaserStream trial)");

    let labels: HashMap<String, &str> =
        VAULTS.iter().map(|(k, v)| (k.to_string(), *v)).collect();

    let request = SubscribeRequest {
        accounts: HashMap::from([(
            "stacsol-vaults".to_string(),
            SubscribeRequestFilterAccounts {
                account: VAULTS.iter().map(|(k, _)| k.to_string()).collect(),
                ..Default::default()
            },
        )]),
        slots: HashMap::from([(
            "slots".to_string(),
            SubscribeRequestFilterSlots { ..Default::default() },
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    eprintln!(
        "[probe] connecting {endpoint} | {} vaults + slot-heartbeat | commitment=processed",
        VAULTS.len()
    );

    let config = LaserstreamConfig::new(endpoint, api_key);
    let (stream, _handle) = subscribe(config, request);
    tokio::pin!(stream);

    let mut n: u64 = 0;
    let mut slots: u64 = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => match update.update_oneof {
                Some(UpdateOneof::Account(acc)) => {
                    if let Some(info) = acc.account {
                        let pk = bs58::encode(&info.pubkey).into_string();
                        let label = labels.get(&pk).copied().unwrap_or("?");
                        n += 1;
                        println!(
                            "[{}] ACCT slot={} wv={} {} lamports={} (#{})",
                            chrono::Utc::now().format("%H:%M:%S%.3f"),
                            acc.slot, info.write_version, label, info.lamports, n
                        );
                    }
                }
                Some(UpdateOneof::Slot(s)) => {
                    slots += 1;
                    if slots <= 5 || slots % 25 == 0 {
                        println!(
                            "[{}] slot-tick {} (heartbeat #{}) — STREAM LIVE",
                            chrono::Utc::now().format("%H:%M:%S%.3f"),
                            s.slot, slots
                        );
                    }
                }
                _ => {}
            },
            Err(e) => eprintln!("[probe] stream error: {e}"),
        }
    }
    Ok(())
}
