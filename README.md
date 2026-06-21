# spsc-ring

A bounded, wait-free **single-producer / single-consumer** ring buffer, implemented twice —
once in **Rust** and once in **C++20** — so the two can be compared directly.

This is the canonical low-latency hand-off: a market-data thread pushing ticks to a strategy
thread without a lock. One thread calls `push`, exactly one other calls `pop`. No mutexes, no
CAS — only acquire/release atomics on a pair of monotonic indices.

```
producer ──push──▶ [ ][x][x][x][ ][ ][ ][ ] ──pop──▶ consumer
                    tail ▲           ▲ head
```

## Layout

| Path    | Language | Build |
|---------|----------|-------|
| `rust/` | Rust     | `cargo` |
| `cpp/`  | C++20    | CMake + Ninja |

Both expose the same shape: a fixed power-of-two capacity, `push` that returns `false` when
full, `pop` that returns `false` when empty, and zero heap allocation after construction.

## Status

Work in progress. See the per-language directories for build and test instructions, and the
[design notes](#design) below as they land.

## License

MIT — see [LICENSE](LICENSE).
