# solana-infra-tools

Low-level Solana infrastructure and latency tooling in Rust, built to support
high-performance trading engines: transaction routing, shred-stream consumption,
gRPC latency probing, and block-engine searching.

> No secrets are stored in code. Endpoints and keys are read from the
> environment at runtime.

## Tools

| Tool | What it is |
|------|------------|
| **tpu-send**          | Sends a signed transaction straight to the current and upcoming leaders' TPU over QUIC, with no block-engine middleman, then polls for landing |
| **shred-consumer**    | Consumes the Solana shred stream for same-slot window detection (sees swaps before they confirm) |
| **laserstream-probe** | gRPC LaserStream latency probe: watches pool reserve vaults and logs each account-update push with a high-precision local timestamp and slot |
| **jito-searcher**     | Jito block-engine searcher (gRPC, `tonic`-generated bindings) for bundle submission |

## Why these matter

Arbitrage and MEV come down to latency and information edge. These tools cover
the wire: getting a transaction to the leader as fast as possible (`tpu-send`),
seeing state changes earlier than RPC (`shred-consumer`, `laserstream-probe`),
and competing for block space (`jito-searcher`).

Each is a focused Rust crate. Build with `cargo build --release`.
