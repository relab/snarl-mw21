# Snarl code for Middleware 2021

## Build
```
go build -o snarl
```

## Usage
```
snarl [command]
```

### Available Commands:
  - **download**    Download and repair a file from Swarm
  - **entangle**    Entangles a file
  - **help**        Help about any command
  - **upload**      Upload a file to Swarm

### Flags:
  * `--bzzkey` `[string]` Bzzkey of account that uploaded content.
  * `--chunkdbpath` `[string]`   Physical location of chunks.
  * `-h`, `--help`                 help for snarl
  * `--ipcpath` `[string]`       Ethereum Inter-process Communications file
  * `--numPeers` `[int]`         Minimum number of peers connected (default 9)
  * `--snarldbpath` `[string]`   Physical location of Snarl chunks.

Use `snarl [command] --help` for more information about a command.
