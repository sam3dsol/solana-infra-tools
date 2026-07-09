#!/usr/bin/env bash
# Run the LaserStream latency probe. Needs LASERSTREAM_API_KEY (from the Helius trial).
# Endpoint defaults to ewr (Newark) = closest to our VPS. Override with LASERSTREAM_ENDPOINT.
export LASERSTREAM_ENDPOINT="${LASERSTREAM_ENDPOINT:-https://laserstream-mainnet-ewr.helius-rpc.com}"
: "${LASERSTREAM_API_KEY:?set LASERSTREAM_API_KEY first (export LASERSTREAM_API_KEY=...)}"
exec nice -n 10 target/release/laserstream-probe
