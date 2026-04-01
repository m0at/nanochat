# jizzrug

`jizzrug` is a polyglot systems language designed for codebases that already live across JavaScript, Go, Rust, and Zig.

The point is not novelty for its own sake. The point is to make polyglot boundaries explicit, reviewable, and compiler-owned instead of scattering them across wrappers, handwritten FFI, and build glue.

This repository is written in `jizzrug`. The source of truth for the language front end, the manifest model, lane aliases, CLI contract, bootstrap roots, and output policy lives in `.jizz` files. A small Node-based bootstrap runtime is included so the `.jizz` source tree can actually be executed today.

This started as a fork of Karpathy's `nanochat`, and honestly forking that repo was inspiring enough that it short-circuited my brain and made me go write an entirely different programming language. The result is not a patch set. It is a full departure motivated by how energizing it was to start from a codebase that felt small, legible, and alive.

License: `AGPL-3.0-only`.

## What It Is

- A language for expressing one program as explicit language-native segments.
- A compiler architecture centered on the `Computer Unified Memory dispatcher`, exposed as `rizz cum`.
- A self-hosting source tree for the parser, manifest model, transpiler, bootstrap plan, and CLI surface.

## Why It Could Matter

- Real systems are already polyglot, but their boundaries are usually informal and hard to reason about.
- Incremental migration is cheaper than rewrites; `jizzrug` is designed around moving subsystem-by-subsystem instead of stack-by-stack.
- Performance-sensitive or safety-sensitive code can live in Rust or Zig while orchestration remains in JavaScript or Go.
- A compiler-owned manifest opens the door to ABI validation, generated interop, capability routing, and build planning.

## Repository Layout

- [jizzrug/source_model.jizz](/Users/andy/nanochat/jizzrug/source_model.jizz): canonical segment and lane model
- [jizzrug/parser.jizz](/Users/andy/nanochat/jizzrug/parser.jizz): parsing rules and parser-stage contracts
- [jizzrug/manifest.jizz](/Users/andy/nanochat/jizzrug/manifest.jizz): emitted plan model
- [jizzrug/transpiler.jizz](/Users/andy/nanochat/jizzrug/transpiler.jizz): lane emission policy
- [rizz/cli.jizz](/Users/andy/nanochat/rizz/cli.jizz): CLI source written in jizzrug
- [rizz/bootstrap.jizz](/Users/andy/nanochat/rizz/bootstrap.jizz): self-hosting bootstrap plan
- [squirt/flush.jizz](/Users/andy/nanochat/squirt/flush.jizz): output flush layer for emitted files and manifests
- [squirt/stream.jizz](/Users/andy/nanochat/squirt/stream.jizz): artifact stream contracts for emitted lanes
- [examples/hello.jizz](/Users/andy/nanochat/examples/hello.jizz): canonical example program
- [bootstrap/runtime.js](/Users/andy/nanochat/bootstrap/runtime.js): host runtime that executes the `.jizz` source tree
- [bin/rizz.js](/Users/andy/nanochat/bin/rizz.js): executable CLI entrypoint
- [package.json](/Users/andy/nanochat/package.json): bootstrap package manifest

## Install

Requirements:

- Node.js 22+

Compatibility note:

- The bootstrap runtime is expected to be portable to OpenBSD in principle, but this has not been verified yet.

Install and expose the CLI locally:

```bash
npm link
```

If you do not want to link it globally, you can run the local entrypoint directly:

```bash
node bin/rizz.js help
```

## CLI

The command surface is defined by [rizz/cli.jizz](/Users/andy/nanochat/rizz/cli.jizz).

Canonical invocation:

```bash
rizz cum <source.jizz> [--out DIR] [--json]
rizz bootstrap [--out DIR] [--json]
```

`cum` stands for `Computer Unified Memory dispatcher`.

Accepted source extensions:

- `.jizz`
- `.jizzrug`
- `.jr`

Canonical examples:

```bash
rizz cum examples/hello.jizz --out dist/hello
rizz cum rizz/cli.jizz --out dist/cli --json
rizz bootstrap --out out/selfhost --json
```

The bootstrap runtime emits:

- `main.js`
- `main.go`
- `main.rs`
- `main.zig`
- `plan.json`

Only lanes present in the input program should be emitted.

The final write step is handled by the `squirt` subsystem, which owns output flushing and manifest delivery.

`rizz bootstrap` walks the `jizzrug`, `rizz`, `squirt`, and `examples` trees, compiles every `.jizz` source unit, and writes a root `bootstrap-plan.json`.

## Syntax

Single-line segments:

```text
js: console.log("back in JavaScript");
go: fmt.Println("hello from Go")
zig: std.debug.print("hello from Zig\n", .{});
```

Fenced blocks:

````text
```rust greet
fn greet(name: &str) -> String {
    format!("hello, {name}")
}
```
````

Supported lane tags:

- `js` / `javascript`
- `go` / `golang`
- `rust` / `rs`
- `zig`

## Status

This repo is now the jizzrug source tree with a working bootstrap runtime.

Today it provides:

- the language-facing source files for the front-end architecture
- a serious CLI contract in jizzrug
- real runtime configuration loaded from the `.jizz` source files
- a canonical example program in `.jizz`
- a Node host runtime that can execute the `.jizz` source tree today
- a bootstrap plan for getting to a self-hosting compiler

What it does not provide yet:

- cross-language type-checking or symbol resolution
- generated FFI or executable multi-language linking
- a true self-hosting compiler implementation

The next meaningful milestone is replacing the bootstrap runtime with a true self-hosting compiler that can reproduce the same outputs from jizzrug source alone.
