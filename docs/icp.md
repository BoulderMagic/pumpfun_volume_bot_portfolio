# Internet Computer Protocol (ICP) — Learning Notes

Reference notes on the Internet Computer, its programming model, and its
tooling. Kept here as background material; this repo's bot itself targets
Solana and is unrelated to ICP infrastructure.

## What ICP is

The Internet Computer (ICP) is a blockchain network, run by the DFINITY
Foundation and independent node providers, that hosts compute and storage
directly on-chain in the form of **canisters** — WebAssembly modules that
combine code and state, similar in spirit to a smart contract but able to
serve HTTP requests, hold gigabytes of persistent memory, and run
arbitrary compute rather than just simple state transitions.

Key differences from account/EVM-style chains:

- **Canisters, not accounts.** Every piece of on-chain logic is a canister
  with its own WASM code and stable memory, addressed by a `Principal`.
- **Reverse gas model.** Canisters pay for their own compute and storage
  in **cycles** (converted from the ICP token), not the caller. Users can
  interact without holding tokens or paying gas per call.
- **Subnets.** The network is partitioned into subnets, each a replicated
  set of nodes running consensus independently; canisters live on one
  subnet and can message canisters on others via inter-canister calls.
- **Chain-key cryptography.** A single public key represents the whole
  network (or a subnet), letting canisters produce threshold-signed
  outputs — including signatures for other chains (see "Chain Fusion"
  below) without a bridge or custodial relayer.

## Programming model

- **Languages**: Motoko (ICP-native, actor-based) or Rust are the two
  primary choices; community CDKs also exist for TypeScript (Azle) and
  Python (Kybra).
- **Actors and messages.** A canister is an actor: it exposes `update`
  methods (state-changing, go through consensus) and `query` methods
  (read-only, fast, answered by a single node and not consensus-backed
  unless called as a "certified query").
- **Async by default.** Inter-canister calls are asynchronous message
  sends; Motoko surfaces this with `async`/`await`, Rust CDK with
  `ic_cdk::call` futures.
- **Stable memory.** Canister heap memory is wiped on upgrade; state
  meant to survive a code upgrade must be written to *stable memory*
  (Motoko's stable variables, or Rust's `ic-stable-structures`).
- **Cycles management.** Canisters must be topped up with cycles or they
  get frozen; cost scales with compute and storage consumed.

## Tooling

- **`dfx`** — the DFINITY Canister SDK CLI: scaffolds projects
  (`dfx new`), builds and deploys canisters locally or to mainnet
  (`dfx deploy`), and manages identities/wallets.
- **`dfx.json`** — per-project manifest declaring canisters, their type
  (motoko/rust/assets/custom), and build config.
- **Candid** — the interface description language ICP uses to describe a
  canister's public API, analogous to a Solidity ABI or a gRPC `.proto`
  file; `dfx` auto-generates `.did` files from source.
- **NNS / Internet Identity** — the Network Nervous System governs the
  protocol itself; Internet Identity is ICP's WebAuthn-based passwordless
  auth system, commonly used for canister-based dApp login.

## Chain Fusion (cross-chain relevance)

ICP's threshold-signature subnets can hold and sign for keys on other
chains without a bridge:

- **Bitcoin**: canisters can hold BTC UTXOs and sign Bitcoin transactions
  directly via the `bitcoin` canister API + threshold ECDSA.
- **Ethereum/EVM**: canisters can sign `secp256k1` transactions for EVM
  chains via threshold ECDSA, and read EVM state via HTTP outcalls or the
  EVM RPC canister.
- **HTTP outcalls**: canisters can make outbound HTTPS calls (with
  replicated, consensus-agreed responses), letting them query
  off-chain/other-chain data — e.g. a price feed or another chain's RPC —
  without an oracle.

There is no equivalent Solana threshold-signature integration as of this
writing; a canister wanting to act on Solana would need to hold and use a
raw Ed25519 keypair itself (via `threshold Schnorr`/Ed25519 signing, now
in preview) or call out to an off-chain signer through HTTP outcalls.

## Minimal example (Motoko)

```motoko
actor Counter {
  stable var count : Nat = 0;

  public func increment() : async Nat {
    count += 1;
    count;
  };

  public query func get() : async Nat {
    count;
  };
};
```

```bash
dfx new counter --type motoko
cd counter
dfx start --background
dfx deploy
dfx canister call counter increment
```

## Glossary

| Term | Meaning |
|---|---|
| Canister | A WASM module + state, ICP's unit of on-chain compute/storage |
| Cycles | The fuel canisters burn for compute/storage, converted from ICP |
| Subnet | A replicated group of nodes running its own consensus instance |
| Principal | The address type identifying users and canisters |
| Candid | The IDL describing a canister's public interface |
| `dfx` | The CLI for building, deploying, and managing canisters |

## Further reading

- https://internetcomputer.org/docs/current/home
- https://internetcomputer.org/docs/current/developer-docs/multi-chain/chain-key-tokens/overview
- https://github.com/dfinity/examples
