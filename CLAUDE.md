# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

RemoteHPC.jl is a Julia package for managing HPC job submission and monitoring across local and remote clusters. It uses a client-daemon architecture: a daemon process runs on the compute cluster (or locally), exposing a REST API, while clients submit jobs and query state over HTTP (tunneled via SSH for remote servers).

## Commands

**Run all tests:**
```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

**Run tests with output:**
```bash
julia --project=. test/runtests.jl
```

**Start a REPL with the package loaded:**
```bash
julia --project=. -e 'using RemoteHPC'
```

**Install/update dependencies:**
```julia
using Pkg; Pkg.instantiate()
```

## Architecture

### Client-Daemon Model

The daemon (`runtime.jl`) runs as a Julia process on the server. Clients (`client.jl`) communicate with it via HTTP. For remote servers, an SSH tunnel is established transparently. Authentication uses a UUID token stored in the server config.

### Core Types (`types.jl`)

- `Server` — target cluster config (hostname, username, port, scheduler type, paths)
- `Exec` — an executable with flags and environment modules
- `Environment` — SLURM/HQ/Bash resource directives (cores, memory, walltime, parallel runner)
- `Calculation` — a single command to run (exec + flags + directory)
- `JobState` — enum with ~22 states (Pending, Running, Completed, Failed, Cancelled, etc.)
- `Storable` — trait interface for persistable objects (Server, Exec, Environment)

### Job Lifecycle

1. `save(server, jobdir, environment, calculations)` — writes `job.sh` and metadata on the server
2. Job enters `full_queue` with `Saved` state
3. `submit(server, jobdir)` — moves job to `submit_queue` with priority
4. Background task in runtime calls the scheduler (`submit(scheduler, jobdir)`) → gets job ID
5. Job enters `current_queue`; another background task syncs states with the scheduler every 5s
6. User queries `state(server, jobdir)` or watches `queue(server)`

### Scheduler Backends (`schedulers.jl`)

Three backends implement `submit`, `abort`, `jobstate`, and `queue`:
- **Bash** — direct bash execution (useful for local testing)
- **Slurm** — `sbatch`/`squeue`/`scancel`
- **HQ (HyperQueue)** — `hq job submit` etc.

### Storage / Database (`database.jl`)

`Storable` objects are JSON-serialized to `~/.julia/config/RemoteHPC/{hostname}/storage/{Type}/{name}.json`. Remote storage goes through HTTP endpoints (`/storage/`). Key functions: `save`, `load`, `exists`, `rm`.

### File Transfer (`servers.jl`)

- `push(local_file, server, remote_file)` — uses `rsync` (handles symlinks, large files, remote SSH)
- `pull(server, remote_file, local_file)` — uses HTTP for small files (<100MB), `scp -r` for large files/directories
- Transparent file I/O: `read(server, path)` / `write(server, path, data)` dispatch to local filesystem or HTTP `/read/` and `/write/` endpoints depending on whether the server is localhost

### REST API (`api.jl`)

File ops: `/read/`, `/write/`, `/rm/`, `/mkpath/`, `/symlink/`, `/cp/`, `/readdir/`, `/mtime/`, `/filesize/`

Job ops: `POST /job/` (save), `PUT /job/` (submit), `GET /job/` (state/info), `POST /abort/`, `PUT /job/priority`

Storage: `/storage/` — CRUD for Storable objects

### Runtime Background Tasks (`runtime.jl`)

Three concurrent tasks managed by `ServerData`:
1. **Queue update** — syncs `current_queue` with scheduler every 5s
2. **Job submission** — drains `submit_queue` by priority, calls scheduler
3. **Connection revival** — rebuilds failed SSH tunnels to other servers

### Known Issues

1. **Low test coverage** — `test/runtests.jl` is a single integration test covering the happy path with Bash/Slurm/HQ schedulers, but edge cases, error handling, and unit-level coverage are sparse.
2. **Pseudo file uploading logic** — The `push`/`pull` strategy in `servers.jl` has correctness issues. `push` always uses rsync (even for small files where HTTP would be simpler/more consistent), while `pull` switches strategy based on file size in a way that may not always be reliable. The "pseudo" file write path (`write(server, path, data)` via HTTP) is a separate code path from `push`, and the two are not always used consistently.
