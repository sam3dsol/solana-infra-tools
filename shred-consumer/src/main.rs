//! shred-consumer v2 — subscribes to the local jito-shredstream-proxy gRPC stream, filters for
//! txs touching watched pools, and fires a UDP "window" signal the instant one appears in the
//! shred stream (same-slot, pre-confirmation).
//!
//! v2 adds ADDRESS LOOKUP TABLE resolution: aggregator-routed swaps reference pools via ALTs, so
//! they don't appear in static_account_keys(). We resolve each v0 tx's address_table_lookups
//! against a cached ALT->addresses map, lazily fetching unknown ALTs off the hot path (aggregators
//! reuse ALTs, so the cache warms within seconds). v1 (static keys only) missed these.
//!
//! Env: WATCH_FILE (accounts to watch; default = built-in stacSOL list), SIGNAL_ADDR (default :9001),
//!      RPC_URL (for ALT fetch; default = read .env).

use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use base64::Engine as _;
use solana_entry::entry::Entry as SolEntry;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;

pub mod shredstream {
    tonic::include_proto!("shredstream");
}
use shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use shredstream::SubscribeEntriesRequest;

const PROXY_GRPC: &str = "http://127.0.0.1:9999";
const SIGNAL_ADDR: &str = "127.0.0.1:9001";
// Raydium CPMM program + swapBaseIn discriminator — lets us parse (pool, amount_in, in_vault)
// straight from a top-level swap in the shred, so the scanner applies x·y=k locally (no RPC).
const CPMM_PROG: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const CPMM_SWAP_BASE_IN: [u8; 8] = [143, 190, 90, 218, 196, 30, 51, 222];
// Meteora DAMM v2 (constant-product). Anchor `global:swap` discriminator; pool@1, in_ata@2, owner@8.
// Direction isn't positional (both vaults fixed), so we ship in_ata+owner and let the scanner decide.
const DAMM_PROG: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
const ANCHOR_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];
// Meteora DLMM swap: disc 41..88; lb_pair@0, amount_in @ data[8]. Direction isn't positional -> engine applies both ways.
const DLMM_PROG: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const DLMM_SWAP: [u8; 8] = [0x41, 0x4b, 0x3f, 0x4c, 0xeb, 0x5b, 0x5b, 0x88];
// Pump AMM: Buy(base_amount_out, max_quote_in) / Sell(base_amount_in, min_quote_out). pool@0, amount @ data[8..16]
// is the EXACT base amount (out for buy, in for sell). Only TOP-LEVEL pump swaps are visible here.
const PUMP_PROG: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMP_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMP_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
// Raydium AMM v4 (legacy CP): swapBaseIn tag=9 (exact-in), pool@1, amount_in@data[1..9], user src/owner at END.
const AMMV4_PROG: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
// Raydium CLMM: swap (global:swap = ANCHOR_SWAP) / swap_v2; pool@2, input_vault@5, amount@data[8..16], is_base_input@data[40].
const RCLMM_PROG: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const ANCHOR_SWAP_V2: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];
// Orca Whirlpool: swap (global:swap); pool@2, vault_a@4, amount@data[8..16], amount_is_input@data[40], a_to_b@data[41].
const WHIRL_PROG: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const WATCHED: &[&str] = &[
    "DK4rgbFv5f4PSZTVmJSqTDL55WwwEjHa9m6xUeh3nRo3",
    "FmyQzcoKX6eZq5s7DuNGX3zpJ27WMjL52JjTC4Gq4yhq",
    "4NboZmfYYJhZkYMc47Quims8TyGLyhTBygA6KrNcrHHW",
    "84FmG9UDjgFS55XnoP3AdZVVW8ukZJZ7jzWDJDd9A5Pv",
    "73edX6xoGY4v5y2hzuKdrUbJXLntqgmo74au1Ki1pump",
    "6K4xdfEk5rvySM496rxm4x8AgC9wVt7N4C7mFFpNAj5f",
];

type Cache = Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>>;

// Fetch an on-chain ALT's address list (addresses live at offset 56, 32 bytes each).
async fn fetch_alt(rpc: &str, alt: Pubkey) -> Option<Vec<Pubkey>> {
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["{}",{{"encoding":"base64"}}]}}"#,
        alt
    );
    let resp = reqwest::Client::new().post(rpc).header("content-type", "application/json")
        .body(body).send().await.ok()?;
    let j: serde_json::Value = resp.json().await.ok()?;
    let b64 = j["result"]["value"]["data"][0].as_str()?;
    let data = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    if data.len() < 56 { return None; }
    Some(data[56..].chunks(32).filter(|c| c.len() == 32).map(|c| Pubkey::try_from(c).unwrap()).collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let watched: Arc<HashSet<Pubkey>> = Arc::new(match std::env::var("WATCH_FILE") {
        Ok(f) => std::fs::read_to_string(&f).unwrap_or_default().lines()
            .filter_map(|l| Pubkey::from_str(l.trim()).ok()).collect(),
        Err(_) => WATCHED.iter().filter_map(|s| Pubkey::from_str(s).ok()).collect(),
    });
    let signal_addr = std::env::var("SIGNAL_ADDR").unwrap_or_else(|_| SIGNAL_ADDR.to_string());
    let rpc = std::env::var("RPC_URL").unwrap_or_else(|_| {
        std::fs::read_to_string(".env").unwrap_or_default().lines()
            .find_map(|l| l.strip_prefix("RPC_URL=").map(|v| v.trim().trim_matches('"').split(" #").next().unwrap_or("").to_string()))
            .unwrap_or_default()
    });
    let sock = UdpSocket::bind("127.0.0.1:0")?;
    let cache: Cache = Arc::new(RwLock::new(HashMap::new()));
    let pending: Arc<Mutex<HashSet<Pubkey>>> = Arc::new(Mutex::new(HashSet::new()));

    eprintln!("[shred-consumer v2] connecting {PROXY_GRPC}, watching {} accounts -> {signal_addr} | ALT-resolve via {}", watched.len(), if rpc.is_empty() {"(no RPC!)"} else {"RPC"});
    let mut client = ShredstreamProxyClient::connect(PROXY_GRPC).await?;
    let mut stream = client.subscribe_entries(SubscribeEntriesRequest {}).await?.into_inner();
    eprintln!("[shred-consumer v2] subscribed — streaming entries, signaling {signal_addr}");

    let mut last_logged_slot = 0u64;
    let mut alt_hits = 0u64;
    while let Some(msg) = stream.message().await? {
        let slot = msg.slot;
        let entries: Vec<SolEntry> = match bincode::deserialize(&msg.entries) { Ok(e) => e, Err(_) => continue };
        for entry in &entries {
            for tx in &entry.transactions {
                // collect WHICH watched pool(s) this tx touches, so the scanner can front-update just them
                let mut moved: Vec<Pubkey> = Vec::new();
                let mut via_alt = false;
                for k in tx.message.static_account_keys().iter() { if watched.contains(k) { moved.push(*k); } }
                // v0 ALT resolution
                if moved.is_empty() {
                    if let VersionedMessage::V0(m) = &tx.message {
                        let guard = cache.read().unwrap();
                        for lk in &m.address_table_lookups {
                            match guard.get(&lk.account_key) {
                                Some(addrs) => {
                                    for &i in lk.writable_indexes.iter().chain(lk.readonly_indexes.iter()) {
                                        if let Some(a) = addrs.get(i as usize) { if watched.contains(a) { moved.push(*a); via_alt = true; } }
                                    }
                                }
                                None => {
                                    // unknown ALT — queue an off-hot-path fetch so future txs resolve
                                    let alt = lk.account_key;
                                    let mut p = pending.lock().unwrap();
                                    if !rpc.is_empty() && p.insert(alt) {
                                        let (c2, rpc2, p2) = (cache.clone(), rpc.clone(), pending.clone());
                                        tokio::spawn(async move {
                                            if let Some(addrs) = fetch_alt(&rpc2, alt).await { c2.write().unwrap().insert(alt, addrs); }
                                            p2.lock().unwrap().remove(&alt);
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                if !moved.is_empty() {
                    let sig = tx.signatures.first().map(|s| s.to_string()).unwrap_or_default();
                    // resolve the full account list (static ++ ALT-writable ++ ALT-readonly) so we can
                    // read top-level swap-ix accounts, then parse CPMM (pool, amount_in, in_vault) deltas.
                    let mut all_keys: Vec<Pubkey> = tx.message.static_account_keys().to_vec();
                    if let VersionedMessage::V0(m) = &tx.message {
                        let guard = cache.read().unwrap();
                        for lk in &m.address_table_lookups { if let Some(a) = guard.get(&lk.account_key) {
                            for &i in &lk.writable_indexes { if let Some(k) = a.get(i as usize) { all_keys.push(*k); } } } }
                        for lk in &m.address_table_lookups { if let Some(a) = guard.get(&lk.account_key) {
                            for &i in &lk.readonly_indexes { if let Some(k) = a.get(i as usize) { all_keys.push(*k); } } } }
                    }
                    let cpmm_prog = Pubkey::from_str(CPMM_PROG).unwrap();
                    let damm_prog = Pubkey::from_str(DAMM_PROG).unwrap();
                    let dlmm_prog = Pubkey::from_str(DLMM_PROG).unwrap();
                    let pump_prog = Pubkey::from_str(PUMP_PROG).unwrap();
                    let ammv4_prog = Pubkey::from_str(AMMV4_PROG).unwrap();
                    let rclmm_prog = Pubkey::from_str(RCLMM_PROG).unwrap();
                    let whirl_prog = Pubkey::from_str(WHIRL_PROG).unwrap();
                    let ata_prog = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
                    let mut delta_pools: HashSet<Pubkey> = HashSet::new();
                    let mut entries: Vec<String> = Vec::new();
                    for ci in tx.message.instructions() {
                        let prog = all_keys.get(ci.program_id_index as usize);
                        let amt = || u64::from_le_bytes(ci.data[8..16].try_into().unwrap());
                        if prog == Some(&cpmm_prog) {
                            // Raydium CPMM swapBaseIn: pool@3, in_vault@6 (a pool vault -> clean match)
                            if ci.data.len() < 16 || ci.data[..8] != CPMM_SWAP_BASE_IN || ci.accounts.len() < 8 { continue; }
                            if let (Some(pool), Some(in_vault)) = (all_keys.get(ci.accounts[3] as usize), all_keys.get(ci.accounts[6] as usize)) {
                                if watched.contains(pool) && delta_pools.insert(*pool) {
                                    entries.push(format!("{pool}~cpmm~{}~{in_vault}", amt()));
                                }
                            }
                        } else if prog == Some(&damm_prog) {
                            // Meteora DAMM v2 swap: pool@1, in_ata@2, out_ata@3, owner@8. Direction isn't
                            // positional, so ship both ATAs + owner; the scanner matches one to the quote ATA.
                            if ci.data.len() < 16 || ci.data[..8] != ANCHOR_SWAP || ci.accounts.len() < 9 { continue; }
                            if let (Some(pool), Some(in_ata), Some(out_ata), Some(owner)) = (
                                all_keys.get(ci.accounts[1] as usize), all_keys.get(ci.accounts[2] as usize),
                                all_keys.get(ci.accounts[3] as usize), all_keys.get(ci.accounts[8] as usize)) {
                                if watched.contains(pool) && delta_pools.insert(*pool) {
                                    entries.push(format!("{pool}~damm~{}~{in_ata}~{out_ata}~{owner}", amt()));
                                }
                            }
                        } else if prog == Some(&dlmm_prog) {
                            // DLMM swap: lb_pair@0, userTokenIn@4, tokenXMint@6, tokenYMint@7, user@10, tokenX/YProgram@11/12.
                            // Direction via ATA-match (no RPC): userTokenIn == ATA(user, tokenX) ? X-in (1) : Y-in (0); non-ATA -> 2 (both).
                            if ci.data.len() < 16 || ci.data[..8] != DLMM_SWAP || ci.accounts.len() < 13 { continue; }
                            let g = |i: usize| all_keys.get(ci.accounts[i] as usize).copied();
                            if let (Some(pool), Some(uin), Some(txm), Some(tym), Some(owner), Some(txp), Some(typ)) =
                                (g(0), g(4), g(6), g(7), g(10), g(11), g(12)) {
                                if watched.contains(&pool) && delta_pools.insert(pool) {
                                    let ata = |o: &Pubkey, m: &Pubkey, tp: &Pubkey| Pubkey::find_program_address(&[o.as_ref(), tp.as_ref(), m.as_ref()], &ata_prog).0;
                                    let dir = if uin == ata(&owner, &txm, &txp) { "1" } else if uin == ata(&owner, &tym, &typ) { "0" } else { "2" };
                                    entries.push(format!("{pool}~dlmm~{}~{dir}", amt()));
                                }
                            }
                        } else if prog == Some(&pump_prog) {
                            // Pump Buy/Sell: pool@0, amount @ data[8..16] = exact BASE amount; side by discriminator.
                            if ci.data.len() < 16 || ci.accounts.is_empty() { continue; }
                            let side = if ci.data[..8] == PUMP_BUY { "b" } else if ci.data[..8] == PUMP_SELL { "s" } else { continue };
                            if let Some(pool) = all_keys.get(ci.accounts[0] as usize) {
                                if watched.contains(pool) && delta_pools.insert(*pool) {
                                    entries.push(format!("{pool}~pump~{}~{side}", amt()));
                                }
                            }
                        } else if prog == Some(&ammv4_prog) {
                            // AMM v4 swapBaseIn (tag 9, exact-in): pool@1, amount_in@data[1..9]; user src/owner at the END.
                            if ci.data.is_empty() || ci.data[0] != 9 || ci.data.len() < 9 || ci.accounts.len() < 3 { continue; }
                            let amt4 = u64::from_le_bytes(ci.data[1..9].try_into().unwrap());
                            let g = |i: usize| all_keys.get(ci.accounts[i] as usize).copied();
                            let n = ci.accounts.len();
                            if let (Some(pool), Some(src), Some(owner)) = (g(1), g(n - 3), g(n - 1)) {
                                if watched.contains(&pool) && delta_pools.insert(pool) {
                                    entries.push(format!("{pool}~ammv4~{amt4}~{src}~{owner}"));
                                }
                            }
                        } else if prog == Some(&rclmm_prog) {
                            // Raydium CLMM swap/swap_v2 (exact-in only): pool@2, input_vault@5, amount@data[8..16], is_base_input@data[40].
                            if ci.data.len() < 41 || (ci.data[..8] != ANCHOR_SWAP && ci.data[..8] != ANCHOR_SWAP_V2) || ci.accounts.len() < 6 || ci.data[40] != 1 { continue; }
                            if let (Some(pool), Some(in_vault)) = (all_keys.get(ci.accounts[2] as usize), all_keys.get(ci.accounts[5] as usize)) {
                                if watched.contains(pool) && delta_pools.insert(*pool) {
                                    entries.push(format!("{pool}~clmm~{}~{in_vault}", amt()));
                                }
                            }
                        } else if prog == Some(&whirl_prog) {
                            // Orca Whirlpool swap (exact-in only): pool@2, vault_a@4, amount@data[8..16], amount_is_input@data[40], a_to_b@data[41].
                            if ci.data.len() < 42 || ci.data[..8] != ANCHOR_SWAP || ci.accounts.len() < 7 || ci.data[40] != 1 { continue; }
                            let atob = if ci.data[41] == 1 { "1" } else { "0" };
                            if let (Some(pool), Some(vault_a)) = (all_keys.get(ci.accounts[2] as usize), all_keys.get(ci.accounts[4] as usize)) {
                                if watched.contains(pool) && delta_pools.insert(*pool) {
                                    entries.push(format!("{pool}~whirlpool~{}~{vault_a}~{atob}", amt()));
                                }
                            }
                        }
                    }
                    // pools we saw move but couldn't parse a CP delta for -> plain entry (scanner refreshes)
                    for m in &moved { if !delta_pools.contains(m) { entries.push(m.to_string()); } }
                    let field3 = entries.join(";");
                    let _ = sock.send_to(format!("{slot}|{sig}|{field3}").as_bytes(), &signal_addr);
                    if via_alt { alt_hits += 1; }
                    if slot != last_logged_slot {
                        let short: String = sig.chars().take(16).collect();
                        eprintln!("[shred-consumer v2] WINDOW slot={slot} sig={short}{}", if via_alt { format!(" (via ALT; alt_hits={alt_hits})") } else { String::new() });
                        last_logged_slot = slot;
                    }
                }
            }
        }
    }
    eprintln!("[shred-consumer v2] stream ended");
    Ok(())
}
