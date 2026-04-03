# jizzrug

`jizzrug` is a polyglot systems language designed for codebases that already live across JavaScript, Go, Rust, and Zig.

The point is not novelty for its own sake. The point is to make polyglot boundaries explicit, reviewable, and compiler-owned instead of scattering them across wrappers, handwritten FFI, and build glue.

This repository is written in `jizzrug`. The source of truth for the language front end, the manifest model, lane aliases, CLI contract, bootstrap roots, and output policy lives in `.jizz` files. A Node-based bootstrap runtime executes the `.jizz` source tree and emits per-lane outputs.

This started as a fork of Karpathy's `nanochat`, and honestly forking that repo was inspiring enough that it short-circuited my brain and made me go write an entirely different programming language. The result is not a patch set. It is a full departure motivated by how energizing it was to start from a codebase that felt small, legible, and alive.

License: `AGPL-3.0-only`.

## What It Is

- A language for expressing one program as explicit language-native segments.
- A compiler architecture centered on the `Computer Unified Memory dispatcher`, exposed as `rizz cum`.
- A self-hosting source tree for the parser, manifest model, transpiler, type system, module system, optimizer, code generator, bootstrap plan, and CLI surface.

## Why It Could Matter

- Real systems are already polyglot, but their boundaries are usually informal and hard to reason about.
- Incremental migration is cheaper than rewrites; `jizzrug` is designed around moving subsystem-by-subsystem instead of stack-by-stack.
- Performance-sensitive or safety-sensitive code can live in Rust or Zig while orchestration remains in JavaScript or Go.
- A compiler-owned manifest opens the door to ABI validation, generated interop, capability routing, and build planning.

## Repository Layout

### Language specification (`jizzrug/`)

- [jizzrug/source_model.jizz](jizzrug/source_model.jizz): canonical segment and lane model
- [jizzrug/parser.jizz](jizzrug/parser.jizz): parsing rules, directives, error recovery, multi-lane syntax
- [jizzrug/manifest.jizz](jizzrug/manifest.jizz): emitted plan model
- [jizzrug/transpiler.jizz](jizzrug/transpiler.jizz): lane emission policy, optimization flags, codegen config
- [jizzrug/types.jizz](jizzrug/types.jizz): type system contracts and cross-lane validation model
- [jizzrug/modules.jizz](jizzrug/modules.jizz): module system, import resolution, dependency graph model

### CLI surface (`rizz/`)

- [rizz/cli.jizz](rizz/cli.jizz): CLI command surface and help text
- [rizz/bootstrap.jizz](rizz/bootstrap.jizz): self-hosting bootstrap plan

### Output subsystem (`squirt/`)

- [squirt/flush.jizz](squirt/flush.jizz): output flush layer for emitted files and manifests
- [squirt/stream.jizz](squirt/stream.jizz): artifact stream contracts for emitted lanes

### Standard library (`stdlib/`)

- [stdlib/io.jizz](stdlib/io.jizz): cross-lane I/O contracts (print, read, file ops)
- [stdlib/fmt.jizz](stdlib/fmt.jizz): cross-lane string formatting (pad, case, join, template)
- [stdlib/math.jizz](stdlib/math.jizz): cross-lane math operations (trig, rounding, clamp, lerp)
- [stdlib/collections.jizz](stdlib/collections.jizz): cross-lane collection types (Stack, Queue, OrderedMap)

Each stdlib module has real implementations in all four lanes (JavaScript, Go, Rust, Zig).

### Bootstrap runtime (`bootstrap/`)

- [bootstrap/runtime.js](bootstrap/runtime.js): host runtime, command routing, compilation pipeline
- [bootstrap/parser.js](bootstrap/parser.js): standalone parser with directives, multi-lane, error recovery
- [bootstrap/typechecker.js](bootstrap/typechecker.js): post-parse validation (labels, refs, lanes, reachability, @type contracts)
- [bootstrap/modules.js](bootstrap/modules.js): module resolution, dependency graph, circular import detection
- [bootstrap/optimizer.js](bootstrap/optimizer.js): AST optimization passes (dead elimination, lane consolidation, comment stripping)
- [bootstrap/codegen.js](bootstrap/codegen.js): FFI stub generation, Makefile generation, interop type declarations
- [bootstrap/cache.js](bootstrap/cache.js): SHA256 content-hash compilation cache
- [bootstrap/watcher.js](bootstrap/watcher.js): fs.watch file watcher for incremental recompilation
- [bootstrap/repl.js](bootstrap/repl.js): interactive REPL with JS lane evaluation
- [bootstrap/fmt.js](bootstrap/fmt.js): source formatter (normalize spacing, collapse blanks, sort imports)

### Other

- [bin/rizz.js](bin/rizz.js): executable CLI entrypoint
- [examples/hello.jizz](examples/hello.jizz): canonical example program
- [package.json](package.json): bootstrap package manifest
- [test/](test/): 102 tests across parser, typechecker, modules, optimizer, codegen, tooling, runtime

## Install

Requirements:

- Node.js 22+

Install and expose the CLI locally:

```bash
npm link
```

If you do not want to link it globally, you can run the local entrypoint directly:

```bash
node bin/rizz.js help
```

## CLI

The command surface is defined by [rizz/cli.jizz](rizz/cli.jizz).

```
usage: rizz <command> [options]

commands:
  cum <source> [--out DIR] [--json]     Run the Computer Unified Memory dispatcher
  bootstrap [--out DIR] [--json]        Compile the repo .jizz source tree
  watch [--out DIR]                     Watch sources and recompile on change
  repl                                  Interactive jizzrug REPL
  check <source>                        Parse and validate without emitting
  fmt <source>                          Format jizzrug source files
  help                                  Show this help text

flags:
  --verbose                             Detailed compilation output
  --dry-run                             Parse and validate but do not write
  --check                               Run typechecker before compilation (with cum)
  --json                                Output manifest as JSON
  --out DIR                             Set output directory
```

`cum` stands for `Computer Unified Memory dispatcher`.

Accepted source extensions: `.jizz`, `.jizzrug`, `.jr`

### Examples

```bash
rizz cum examples/hello.jizz --out dist/hello
rizz cum rizz/cli.jizz --out dist/cli --json
rizz cum examples/hello.jizz --check --verbose
rizz bootstrap --out out/selfhost --json
rizz check examples/hello.jizz
rizz fmt examples/hello.jizz --dry-run
rizz watch --out dist/watch --verbose
rizz repl
```

The bootstrap runtime emits per-lane output files and a plan manifest:

- `main.js`, `main.go`, `main.rs`, `main.zig` (only lanes present in the input)
- `plan.json`

The final write step is handled by the `squirt` subsystem, which owns output flushing and manifest delivery.

`rizz bootstrap` walks the `jizzrug`, `rizz`, `squirt`, `stdlib`, and `examples` trees, compiles every `.jizz` source unit, and writes a root `bootstrap-plan.json`.

## Syntax

### Single-line segments

```text
js: console.log("back in JavaScript");
go: fmt.Println("hello from Go")
zig: std.debug.print("hello from Zig\n", .{});
```

### Multi-lane inline segments

```text
js,go: // shared concept across lanes
```

### Fenced blocks

````text
```rust greet
fn greet(name: &str) -> String {
    format!("hello, {name}")
}
```
````

### Multi-lane fenced blocks

````text
```js,rust shared_label
// emitted as separate segments for each lane
```
````

### Supported lane tags

- `js` / `javascript` / `node`
- `go` / `golang`
- `rust` / `rs`
- `zig`

### Directives

```text
@import "stdlib/io.jizz"
@import "./other_module.jizz"
@meta author andy
@meta version 0.1.0
@ref greet
@type greet: string -> string
```

- `@import` declares a module dependency (relative paths or `stdlib/` prefix)
- `@meta` attaches file-level key-value metadata
- `@ref` declares a reference to a labeled segment (validated by typechecker)
- `@type` declares a cross-lane type contract (validated by typechecker)

## Pipeline

The compilation pipeline runs in this order:

1. **Parse** -- `.jizz` source to AST (segments, imports, meta, refs)
2. **Typecheck** -- validate lanes, labels, refs, reachability, type contracts (optional, `--check`)
3. **Optimize** -- dead segment elimination, lane consolidation, empty removal, comment stripping (optional, `--optimize`)
4. **Dispatch and emit** -- group segments by lane, render per-lane output via transpiler
5. **Flush** -- write output files and plan manifest via squirt subsystem

## Module System

Files can import other `.jizz` modules:

```text
@import "stdlib/io.jizz"
@import "./helpers.jizz"
```

The module resolver handles:
- Relative path resolution from the importing file
- `stdlib/` prefix resolution from the repo root
- Automatic `.jizz` extension appending
- Circular import detection with cycle path reporting
- Dependency graph construction and topological ordering

## Type System

The typechecker validates cross-lane contracts:

- All lane tags must be one of the four supported lanes
- Segment labels must be unique within a file
- `@ref` targets must resolve to existing labeled segments
- Adjacent same-lane segments after a terminating statement are flagged as unreachable
- `@type` contracts declare cross-lane function signatures (`@type name: param -> param -> return`)
- Duplicate type contracts are rejected

## Status

This repo is the jizzrug source tree with a working bootstrap runtime, standard library, type system, module system, optimizer, code generator, and developer tooling.

Today it provides:

- the language-facing source files for the full front-end architecture
- a parser with directives, multi-lane syntax, column tracking, and error recovery
- a post-parse typechecker with cross-lane contract validation
- a module system with import resolution, dependency graphs, and circular import detection
- a standard library with real implementations across all four lanes
- an optimizer with dead elimination, lane consolidation, and comment stripping
- a code generator with FFI stubs, Makefiles, and interop type declarations
- a SHA256 build cache for incremental compilation
- a file watcher for live recompilation
- an interactive REPL with JS lane evaluation
- a source formatter
- a CLI with 6 commands and 5 flags
- 102 tests across all subsystems

What it does not provide yet:

- cross-language type-checking or symbol resolution at the semantic level
- generated FFI or executable multi-language linking
- a true self-hosting compiler implementation

The next meaningful milestone is replacing the bootstrap runtime with a true self-hosting compiler that can reproduce the same outputs from jizzrug source alone.
