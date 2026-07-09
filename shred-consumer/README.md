# shred-consumer — same-slot window detection for the stacSOL arb bot

Closes the ~1-slot detection gap that makes us lose contested windows. Jito ShredStream
feeds us txs as the leader produces them; this consumer spots a pool-moving tx and fires a
UDP signal to the Node bot BEFORE the tx is confirmed — so we fire into the same slot.

## Pipeline
    Jito ShredStream (NY)  --auth via shredstream-auth.json-->
      jito-shredstream-proxy   [reconstructs shreds -> entries, serves gRPC :9999]
        -> shred-consumer (this)  [subscribe -> filter our pools -> UDP :9001]
          -> Node bot (arb.js)    [fires immediately]

## PREREQUISITES (gating)
1. Bigger box — proxy needs 4-8 cores to reconstruct the mainnet firehose. Current 1-core
   box cannot. Provision DO CPU-Optimized 8 vCPU/16GB, NY region; migrate bots there.
2. Jito ShredStream approval for pubkey F9pYc1efenv5uNP2RGvqhoiRoDw61WYZVmvMHZDna8RT (pending).

## BUILD (on the upgraded box)
    # 1. Rust toolchain
    curl https://sh.rustup.rs -sSf | sh -s -- -y && source ~/.cargo/env
    apt-get install -y protobuf-compiler pkg-config libssl-dev
    # 2. Build this consumer
    cd shred-consumer && cargo build --release
    #   -> ./target/release/shred-consumer
    #   If bincode deserialize of Entry fails at runtime, bump solana-entry/solana-sdk in
    #   Cargo.toml to match the cluster version, rebuild.

## RUN (after approval)
    # A. Jito proxy (clone + build jito-labs/shredstream-proxy, or use their Docker image):
    ./jito-shredstream-proxy shredstream \
      --block-engine-url https://ny.mainnet.block-engine.jito.wtf \
      --auth-keypair shredstream-auth.json \
      --desired-regions ny \
      --grpc-service-port 9999
      # verify exact flags with `jito-shredstream-proxy shredstream --help`
    # B. the consumer:
    target/release/shred-consumer

## NODE INTEGRATION (arb.js)
Import the trigger and fire on signal instead of waiting for the poll:

    import { startShredTrigger } from "./shredTrigger.mjs";
    // after the fire loop is set up, reuse the SAME fire path:
    startShredTrigger(async () => {
      if (firing || Date.now() < jupBackoffUntil ||
          Date.now() - lastBundleMs < MIN_SEND_GAP_MS) return;
      const opps = await checkOpportunity();
      if (opps && opps.length) { /* ...existing send path... */ }
    });

Run proxy + consumer under pm2 alongside the bot, all on the same box (signal stays local).

## v2 TODOs (after v1 lands windows)
- ALT resolution: txs referencing our pools via Address Lookup Tables wont match
  static_account_keys(). Resolve+cache the ALTs and check loaded addresses too.
- Parse the opener swap amounts to compute the NEW reserves and size the fire exactly
  (instead of firing on detection with the bots current estimate).
- For ultimate speed, move the fire path itself into Rust so detect->send never crosses
  into Node. (Biggest latency win, biggest rewrite — only if v1 proves the edge.)
