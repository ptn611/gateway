# Rust version

Latest rust stable. `1.89` or newer is required by the dependency graph.

```
rustup default stable
```

The old `<=1.76` / `x86_64` pin was a constraint of the `libdrift_ffi_sys` build and no
longer applies -- the program is now a direct rust dependency via `velocity-rs`.


# Tests

Most tests hit the network and expect the `VELOCITY_GATEWAY_KEY` environment variable to
be set, to a key with a valid Velocity account on devnet. The pure unit tests (order
scaling, wire formats, swift message encoding) run without any of this.

Optional overrides:

| Variable | Description |
|----------|-------------|
| `TEST_RPC_ENDPOINT` | devnet RPC to use (default: `https://api.devnet.solana.com`) |
| `TEST_MAINNET_RPC_ENDPOINT` | mainnet RPC, used by the swap tests |
| `TEST_DELEGATED_SIGNER` | delegate key for the delegated signing tests |

Devnet fauct:
* https://faucet.devnet.solana.com/
* `solana airdrop 2 <your_pubkey>`

Initialize an account on https://beta.velocity.exchange, and set your browser wallet
to point to devnet.

Run tests with:
```
cargo test
```
