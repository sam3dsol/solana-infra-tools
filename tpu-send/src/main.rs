// tpu-send spike: send a signed tx STRAIGHT to the leader's TPU over QUIC (no block-engine middleman).
// Test: fire a 1-lamport self-transfer to the current + next few leaders, then poll if it landed.
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use base64::Engine as _;
use serde_json::{json, Value};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
    hash::Hash,
};
use std::str::FromStr;

const COMPUTE_PID: &str = "ComputeBudget111111111111111111111111111111";

fn env_val(k: &str) -> String {
    for line in std::fs::read_to_string(".env").unwrap_or_default().lines() {
        if let Some(eq) = line.find('=') {
            if line[..eq].trim() == k {
                let mut v = line[eq + 1..].to_string();
                if let Some(h) = v.find(" #") { v.truncate(h); }
                return v.trim().trim_matches('"').to_string();
            }
        }
    }
    String::new()
}

async fn rpc(c: &reqwest::Client, url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
    match c.post(url).json(&body).send().await {
        Ok(r) => r.json().await.unwrap_or(Value::Null),
        Err(e) => { eprintln!("rpc {method} err {e}"); Value::Null }
    }
}

fn cu_limit(units: u32) -> Instruction {
    let mut d = vec![2u8]; d.extend_from_slice(&units.to_le_bytes());
    Instruction { program_id: Pubkey::from_str(COMPUTE_PID).unwrap(), accounts: vec![], data: d }
}
fn cu_price(micro_lamports: u64) -> Instruction {
    let mut d = vec![3u8]; d.extend_from_slice(&micro_lamports.to_le_bytes());
    Instruction { program_id: Pubkey::from_str(COMPUTE_PID).unwrap(), accounts: vec![], data: d }
}

// Solana TPU QUIC client config: dummy ed25519 cert + ALPN "solana-tpu" + skip server verify.
fn quic_client_config(identity: &Keypair) -> quinn::ClientConfig {
    let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(identity);
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(solana_tls_utils::SkipServerVerification::new())
        .with_client_auth_cert(vec![cert], key)
        .expect("client auth cert");
    crypto.alpn_protocols = vec![b"solana-tpu".to_vec()];
    crypto.enable_early_data = true;
    let qcc = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).expect("quic crypto");
    let mut cfg = quinn::ClientConfig::new(Arc::new(qcc));
    let mut tp = quinn::TransportConfig::default();
    tp.max_concurrent_bidi_streams(0u32.into());
    tp.max_concurrent_uni_streams(1u32.into());
    tp.max_idle_timeout(Some(Duration::from_secs(2).try_into().unwrap()));
    tp.keep_alive_interval(Some(Duration::from_millis(500)));
    cfg.transport_config(Arc::new(tp));
    cfg
}

async fn tpu_send(ep: &quinn::Endpoint, addr: SocketAddr, wire: &[u8]) -> Result<(), String> {
    let conn = ep.connect(addr, "tpu").map_err(|e| format!("connect cfg {e}"))?
        .await.map_err(|e| format!("handshake {e}"))?;
    let mut s = conn.open_uni().await.map_err(|e| format!("open_uni {e}"))?;
    s.write_all(wire).await.map_err(|e| format!("write {e}"))?;
    s.finish().map_err(|e| format!("finish {e}"))?;
    // give the data time to flush before closing
    tokio::time::sleep(Duration::from_millis(120)).await;
    conn.close(0u32.into(), b"done");
    Ok(())
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider().install_default().ok();
    let url = env_val("RPC_URL");
    let wallet = Keypair::from_bytes(&bs58::decode(env_val("PRIVATE_KEY")).into_vec().expect("b58 key")).expect("keypair");
    let identity = Keypair::new(); // unstaked QUIC identity
    let c = reqwest::Client::new();
    println!("wallet {}", wallet.pubkey());

    // 1) current slot + next leaders
    let slot = rpc(&c, &url, "getSlot", json!([])).await["result"].as_u64().unwrap();
    let leaders = rpc(&c, &url, "getSlotLeaders", json!([slot, 12])).await;
    let leaders: Vec<String> = leaders["result"].as_array().unwrap().iter()
        .map(|v| v.as_str().unwrap().to_string()).collect();
    // distinct leaders in upcoming order
    let mut distinct: Vec<String> = Vec::new();
    for l in &leaders { if !distinct.contains(l) { distinct.push(l.clone()); } }
    println!("slot {slot} | upcoming distinct leaders: {}", distinct.len());

    // 2) cluster nodes -> pubkey -> tpuQuic
    let nodes = rpc(&c, &url, "getClusterNodes", json!([])).await;
    let mut tpu: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for n in nodes["result"].as_array().unwrap() {
        if let (Some(pk), Some(q)) = (n["pubkey"].as_str(), n["tpuQuic"].as_str()) {
            tpu.insert(pk.to_string(), q.to_string());
        }
    }
    let targets: Vec<SocketAddr> = distinct.iter().take(4)
        .filter_map(|pk| tpu.get(pk).and_then(|a| a.parse().ok()))
        .collect();
    println!("resolved {} leader TPU-QUIC targets: {:?}", targets.len(), targets);
    if targets.is_empty() { eprintln!("no TPU targets resolved"); return; }

    // 3) build a tiny self-transfer with a priority fee
    let bh = rpc(&c, &url, "getLatestBlockhash", json!([{"commitment":"processed"}])).await;
    let bh = Hash::from_str(bh["result"]["value"]["blockhash"].as_str().unwrap()).unwrap();
    let ixs = vec![
        cu_limit(450),
        cu_price(50_000), // 0.0000225 SOL priority on 450 CU ~ negligible, just for inclusion
        system_instruction::transfer(&wallet.pubkey(), &wallet.pubkey(), 1),
    ];
    let msg = Message::new(&ixs, Some(&wallet.pubkey()));
    let mut tx = Transaction::new_unsigned(msg);
    tx.sign(&[&wallet], bh);
    let wire = bincode::serialize(&tx).unwrap();
    let sig = tx.signatures[0].to_string();
    println!("tx sig {sig} | wire {} bytes", wire.len());

    // 4) QUIC endpoint + blast to all target leaders concurrently
    let cfg = quic_client_config(&identity);
    let mut ep = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    ep.set_default_client_config(cfg);
    let t0 = std::time::Instant::now();
    let mut handles = Vec::new();
    for addr in targets.clone() {
        let ep2 = ep.clone(); let w = wire.clone();
        handles.push(tokio::spawn(async move {
            let r = tpu_send(&ep2, addr, &w).await;
            (addr, r)
        }));
    }
    for h in handles {
        if let Ok((addr, r)) = h.await { println!("  -> {addr}: {}", match r { Ok(_) => "SENT".to_string(), Err(e) => format!("ERR {e}") }); }
    }
    println!("send wall {} ms", t0.elapsed().as_millis());

    // 5) poll landing
    for i in 0..20 {
        tokio::time::sleep(Duration::from_millis(700)).await;
        let st = rpc(&c, &url, "getSignatureStatuses", json!([[sig]])).await;
        let v = &st["result"]["value"][0];
        if !v.is_null() {
            let err = &v["err"];
            println!("LANDED after ~{}ms | status={}", (i+1)*700, if err.is_null() {"SUCCESS".into()} else {format!("ERR {err}")});
            return;
        }
    }
    println!("NOT LANDED within ~14s (direct-TPU did not get it included)");
}
