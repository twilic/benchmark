# Contributing

Thank you for improving the Twilic benchmark harness.

## Scope

This repository measures encode/decode performance for `twilic-js` and comparison formats. Changes should keep benchmark scenarios reproducible and documented in `README.md`.

## Development

Requirements:

- Node.js 24+
- A built `twilic-js` package at `../twilic-js`

```bash
pnpm install
pnpm --dir ../twilic-js build
pnpm bench
pnpm typecheck
pnpm format
```

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/).

Use this format:

```text
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common types include `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, and `chore`.

Examples:

- `feat: add wasm backend warmup flag`
- `fix: correct batch size in max mode`

After `pnpm install`, Husky runs Commitlint on each local commit. Pull requests are also checked in CI so every commit in the branch follows the same rules.

## Contribution Checklist

- Benchmark output or flags are documented when behavior changes
- `pnpm typecheck` passes locally
- Commit messages follow Conventional Commits

By contributing to this repository, you agree that your contribution may be distributed under the MIT license used by the project.
