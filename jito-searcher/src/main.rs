//! Jito Block Engine SEARCHER gRPC client — auth (challenge -> sign -> tokens) + SendBundle.
//! Signs with the dedicated auth keypair (ed25519). Run it to prove gRPC auth end-to-end
//! (auth + GetTipAccounts). send_bundle_grpc() is the bundle-submission path.
use std::fs;
use ed25519_dalek::{Signer, SigningKey};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::Request;

pub mod auth { tonic::include_proto!("auth"); }
pub mod shared { tonic::include_proto!("shared"); }
pub mod packet { tonic::include_proto!("packet"); }
pub mod bundle { tonic::include_proto!("bundle"); }
pub mod searcher { tonic::include_proto!("searcher"); }

use auth::auth_service_client::AuthServiceClient;
use auth::{GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role};
use searcher::searcher_service_client::SearcherServiceClient;
use searcher::{GetTipAccountsRequest, SendBundleRequest};
use bundle::Bundle;
use packet::Packet;

// load a Solana 64-byte keypair JSON ([seed32 || pubkey32]) and return (signing key, pubkey bytes)
fn load_keypair(path: &str) -> (SigningKey, [u8; 32]) {
    let bytes: Vec<u8> = serde_json::from_str(&fs::read_to_string(path).expect("read keypair")).expect("parse keypair");
    let seed: [u8; 32] = bytes[0..32].try_into().expect("seed");
    let pubkey: [u8; 32] = bytes[32..64].try_into().expect("pubkey");
    (SigningKey::from_bytes(&seed), pubkey)
}

// wrap any request with the Bearer access-token metadata (authed gRPC call)
fn authed<T>(msg: T, bearer: &MetadataValue<Ascii>) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", bearer.clone());
    r
}

// === SUBMIT A BUNDLE over gRPC: wrap a signed, serialized tx in a single-tx bundle ===
pub async fn send_bundle_grpc(
    searcher: &mut SearcherServiceClient<Channel>,
    bearer: &MetadataValue<Ascii>,
    signed_tx_bytes: Vec<u8>,
) -> Result<String, Box<dyn std::error::Error>> {
    let bundle = Bundle { header: None, packets: vec![Packet { data: signed_tx_bytes, meta: None }] };
    let resp = searcher.send_bundle(authed(SendBundleRequest { bundle: Some(bundle) }, bearer)).await?;
    Ok(resp.into_inner().uuid)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("BLOCK_ENGINE_URL").unwrap_or_else(|_| "https://ny.mainnet.block-engine.jito.wtf".into());
    let kp = std::env::var("AUTH_KEYPAIR").unwrap_or_else(|_| "shredstream-auth.json".into());
    let (signing, pubkey) = load_keypair(&kp);
    let pubkey_b58 = bs58::encode(pubkey).into_string();
    println!("searcher auth pubkey: {}", pubkey_b58);

    // 1) TLS channel to the Block Engine
    let channel = Channel::from_shared(url.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect().await?;

    // 2) gRPC AUTH: GenerateAuthChallenge -> sign("{pubkey}-{challenge}") -> GenerateAuthTokens
    let mut auth = AuthServiceClient::new(channel.clone());
    let challenge = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest { role: Role::Searcher as i32, pubkey: pubkey.to_vec() })
        .await?.into_inner().challenge;
    let to_sign = format!("{}-{}", pubkey_b58, challenge);
    let sig = signing.sign(to_sign.as_bytes());
    let tokens = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_vec(),
            signed_challenge: sig.to_bytes().to_vec(),
        })
        .await?.into_inner();
    let access = tokens.access_token.ok_or("no access token")?.value;
    println!("gRPC auth OK — access token ({} chars)", access.len());

    // 3) authed SearcherService — prove access with GetTipAccounts
    let bearer: MetadataValue<Ascii> = format!("Bearer {}", access).parse()?;
    let mut searcher = SearcherServiceClient::new(channel);
    let tips = searcher.get_tip_accounts(authed(GetTipAccountsRequest {}, &bearer)).await?.into_inner().accounts;
    println!("authed SearcherService.GetTipAccounts OK — {} tip accounts; first = {}", tips.len(), tips.first().cloned().unwrap_or_default());

    // To send: searcher.send_bundle(...) via send_bundle_grpc(&mut searcher, &bearer, signed_tx_bytes).await
    Ok(())
}
