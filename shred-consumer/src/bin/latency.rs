//! shred-latency — measures the ShredStream EDGE: for each watched-pool tx seen in the shred stream
//! (pre-confirmation), record receipt time, then poll the RPC's getSignatureStatuses until the same
//! sig shows up at `processed` (the view the bot currently detects on). lead = t_rpc - t_shred.
//! Read-only; no spend, no tx. Env: WATCH_FILE, RPC_URL (or .env).
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::message::VersionedMessage;
use solana_entry::entry::Entry as SolEntry;

pub mod shredstream { tonic::include_proto!("shredstream"); }
use shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use shredstream::SubscribeEntriesRequest;

async fn rpc_seen(rpc: &str, sig: &str) -> bool {
    let body = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["{}"],{{"searchTransactionHistory":false}}]}}"#, sig);
    if let Ok(r) = reqwest::Client::new().post(rpc).header("content-type","application/json").body(body).send().await {
        if let Ok(j) = r.json::<serde_json::Value>().await {
            return !j["result"]["value"][0].is_null();
        }
    }
    false
}

#[tokio::main]
async fn main() -> Result<()> {
    let watched: Arc<HashSet<Pubkey>> = Arc::new(std::fs::read_to_string(std::env::var("WATCH_FILE").unwrap_or_default())
        .unwrap_or_default().lines().filter_map(|l| Pubkey::from_str(l.trim()).ok()).collect());
    let rpc = std::env::var("RPC_URL").unwrap_or_else(|_| std::fs::read_to_string(".env").unwrap_or_default()
        .lines().find_map(|l| l.strip_prefix("RPC_URL=").map(|v| v.trim().trim_matches('"').split(" #").next().unwrap_or("").to_string())).unwrap_or_default());
    let samples: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(vec![]));
    let seen_sigs: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    eprintln!("[latency] watching {} accts | RPC {} | measuring ShredStream lead over RPC-processed", watched.len(), if rpc.is_empty(){"(none!)"}else{"set"});
    let mut client = ShredstreamProxyClient::connect("http://127.0.0.1:9999").await?;
    let mut stream = client.subscribe_entries(SubscribeEntriesRequest{}).await?.into_inner();
    eprintln!("[latency] subscribed");
    while let Some(msg) = stream.message().await? {
        let entries: Vec<SolEntry> = match bincode::deserialize(&msg.entries){Ok(e)=>e,Err(_)=>continue};
        for entry in &entries { for tx in &entry.transactions {
            let mut hit = tx.message.static_account_keys().iter().any(|k| watched.contains(k));
            if !hit { if let VersionedMessage::V0(m)=&tx.message {
                // (latency probe: static-key match is enough to time the edge; ALT-resolve omitted for simplicity)
                let _ = m; } }
            if !hit { continue; }
            let sig = match tx.signatures.first(){Some(s)=>s.to_string(),None=>continue};
            { let mut ss = seen_sigs.lock().unwrap(); if !ss.insert(sig.clone()) { continue; } }
            let t0 = Instant::now();
            let (rpc2, samp, sigc) = (rpc.clone(), samples.clone(), sig.clone());
            tokio::spawn(async move {
                // poll RPC until it sees the sig at processed, or give up after 3s
                loop {
                    if t0.elapsed().as_millis() > 3000 { return; }
                    if rpc_seen(&rpc2, &sigc).await {
                        let lead = t0.elapsed().as_millis();
                        let mut v = samp.lock().unwrap(); v.push(lead);
                        let mut s: Vec<u128> = v.clone(); s.sort();
                        let med = s[s.len()/2]; let avg = s.iter().sum::<u128>()/s.len() as u128;
                        eprintln!("[latency] n={} ShredStream lead over RPC-processed: this={}ms median={}ms avg={}ms", s.len(), lead, med, avg);
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            });
        }}
    }
    Ok(())
}
