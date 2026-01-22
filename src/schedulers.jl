abstract type Scheduler end

@kwdef struct Bash <: Scheduler
    type::String = "bash"
end

@kwdef struct Slurm <: Scheduler
    type::String = "slurm"
end

@kwdef struct HQ <: Scheduler
    type::String = "hq"
    server_command::String = "hq"
    allocs::Vector{String} = String[]
end

StructTypes.StructType(::Type{Cmd}) = StructTypes.Struct()
StructTypes.StructType(::Type{Scheduler}) = StructTypes.AbstractType()
StructTypes.subtypes(::Type{Scheduler}) = (bash = Bash, slurm = Slurm, hq = HQ)

StructTypes.StructType(::Type{Bash}) = StructTypes.Struct()
StructTypes.StructType(::Type{Slurm}) = StructTypes.Struct()
StructTypes.StructType(::Type{HQ}) = StructTypes.Struct()
StructTypes.subtypekey(::Type{Scheduler}) = :type

function Scheduler(d::Dict{String,Any})
    t = d["type"]
    if t == "bash"
        return Bash(t)
    elseif t == "slurm"
        return Slurm(t)
    else
        return HQ(t, d["server_command"], string.(d["allocs"]))
    end
end
Base.convert(::Type{Scheduler}, d::Dict) = Scheduler(d)

function submit(::S, ::AbstractString) where {S<:Scheduler}
    return error("No submit method defined for $S.")
end
abort(::S, ::Int) where {S<:Scheduler} = error("No abort method defined for $S.")
jobstate(::S, ::Any) where {S<:Scheduler} = error("No jobstate method defined for $S.")

submit_cmd(s::S) where {S<:Scheduler} = error("No submit_cmd method defined for $S.")
submit_cmd(s::Slurm) = "sbatch"
submit_cmd(s::Bash) = "bash"
submit_cmd(s::HQ) = "hq"

function is_reachable(server_command::String)
    t = run(string2cmd("which $server_command"); wait = false)
    while !process_exited(t)
        sleep(0.005)
    end
    return t.exitcode == 0
end
is_reachable(s::HQ) = is_reachable(s.server_command)
is_reachable(::Slurm) = is_reachable("sbatch")
is_reachable(::Bash) = true

directive_prefix(::Slurm) = "#SBATCH"
directive_prefix(::Bash) = "#"
directive_prefix(::HQ) = "#HQ"

name_directive(::Slurm) = "#SBATCH --job-name"
name_directive(::Bash) = "# job-name"
name_directive(::HQ) = "#HQ --name"

function parse_params(sched::Scheduler, preamble::String)
    m = match(r"$(directive_prefix(sched)) (.+)\n", preamble)
    params = Dict()
    last = 0
    while m !== nothing
        s = split(replace(replace(m.captures[1], "-" => ""), "=" => " "))
        tp = Meta.parse(s[2])
        t = tp isa Symbol || tp isa Expr ? s[2] : tp
        params[s[1]] = t
        last = m.offset + length(m.match)
        m = match(r"$(directive_prefix(sched)) (.+)\n", preamble, last)
    end
    return params, preamble[last:end]
end

maybe_scheduler_restart(::Bash) = false

function queue(::Bash)
    return Dict()
    # TODO this is not working with how queue is updated
    # joblog = config_path("jobs")
    # qd = JSON3.read(read(joinpath(joblog, "queue.json"), String))
    # return merge(
    #     parse_submit_queue(qd.submit_queue),
    #     parse_current_queue(qd.current_queue),
    #     # not needed for now
    #     # d = parse_queue(qd.full_queue)
    # )
end

function parse_submit_queue(qdict)
    d = Dict{String, Tuple{Int64, JobState}}()
    for k in keys(qdict)
        v = qdict[k]
        priority, _ = v[1], v[2]
        d[string(k)] = (priority, Pending)
    end
    return d
end

function parse_current_queue(qdict)
    d = Dict{String, Tuple{Int64, JobState}}()
    for k in keys(qdict)
        v = qdict[k]
        id, state = Int64(v.id), str2state[v.state]
        d[string(k)] = (id, state)
    end
    return d
end

function jobstate(::Bash, id::Int)
    out = Pipe()
    err = Pipe()
    p = run(pipeline(ignorestatus(`ps -p $id`); stderr = err, stdout = out))
    close(out.in)
    close(err.in)
    return p.exitcode == 1 ? Completed : Running
end

function submit(::Bash, j::AbstractString)
    return Int(getpid(run(Cmd(`bash job.sh`; detach = true, dir = j); wait = false)))
end

function abort(::Bash, id::Int)
    if Sys.islinux()
        pids = [parse(Int, split(s)[1]) for s in readlines(`ps -s $id`)[2:end]]
    elseif Sys.isapple()
        pids = [parse(Int, split(s)[1]) for s in readlines(`ps -p $id -o pgid=`)]
    end
    for p in pids
        run(ignorestatus(`kill $p`))
    end
end

function in_queue(s::JobState)
    return s in (Submitted, Pending, Running, Configuring, Completing, Suspended)
end

## SLURM ##
function maybe_scheduler_restart(::Slurm)
    if occursin("error", read(`squeue -u $(ENV["USER"])`, String))
        if occursin("(null)", read(`slurmd`, String))
            error("Can not start slurmctld automatically...")
        else
            return true
        end
    else
        return false
    end
end

function queue(sc::Slurm)
    qlines = readlines(`squeue -u $(ENV["USER"]) --format="%Z %i %T"`)[2:end]
    result = Dict{String, Tuple{Int, JobState}}()
    for x in qlines
        parts = split(x)
        if length(parts) >= 3
            dir = parts[1]
            id = parse(Int, parts[2])
            state = jobstate(sc, parts[3])
            result[dir] = (id, state)
            @debug "squeue: dir=$dir, id=$id, state=$state" logtype=RuntimeLog
        end
    end
    return result
end

function jobstate(s::Slurm, id::Int)
    cmd = `sacct -u $(ENV["USER"]) --format=State -j $id -P -n`
    st = Unknown
    try
        lines = readlines(cmd)
        @debug "sacct output for job $id: $lines" logtype=RuntimeLog
        if !isempty(lines)
            # Take first non-empty state (job state, not step states)
            state_str = strip(first(lines))
            st = jobstate(s, state_str)
            @debug "Parsed state for job $id: $st (from '$state_str')" logtype=RuntimeLog
        end
    catch e
        @debug "sacct failed for job $id: $e" logtype=RuntimeLog
    end
    st != Unknown && return st

    cmd = `scontrol show job $id`
    try
        output = read(cmd, String)
        reg = r"JobState=(\w+)\b"
        m = match(reg, output)
        if m !== nothing
            st = jobstate(s, m[1])
            @debug "scontrol state for job $id: $st (from '$(m[1])')" logtype=RuntimeLog
            return st
        end
    catch e
        @debug "scontrol failed for job $id: $e" logtype=RuntimeLog
    end
    @debug "Could not determine state for job $id, returning Unknown" logtype=RuntimeLog
    return Unknown
end

function jobstate(::Slurm, state::AbstractString)
    # SLURM states can have suffixes like "CANCELLED by 12345" or "COMPLETED+"
    # Use startswith for robust matching
    if startswith(state, "PENDING")
        return Pending
    elseif startswith(state, "RUNNING")
        return Running
    elseif startswith(state, "COMPLETED")
        return Completed
    elseif startswith(state, "CONFIGURING")
        return Configuring
    elseif startswith(state, "COMPLETING")
        return Completing
    elseif startswith(state, "CANCELLED")
        return Cancelled
    elseif startswith(state, "BOOT_FAIL")
        return BootFail
    elseif startswith(state, "DEADLINE")
        return Deadline
    elseif startswith(state, "FAILED")
        return Failed
    elseif startswith(state, "NODE_FAIL")
        return NodeFail
    elseif startswith(state, "OUT_OF_MEMORY")
        return OutOfMemory
    elseif startswith(state, "PREEMPTED")
        return Preempted
    elseif startswith(state, "REQUEUED")
        return Requeued
    elseif startswith(state, "RESIZING")
        return Resizing
    elseif startswith(state, "REVOKED")
        return Revoked
    elseif startswith(state, "SUSPENDED")
        return Suspended
    elseif startswith(state, "TIMEOUT")
        return Timeout
    end
    return Unknown
end

function submit(::Slurm, j::AbstractString)
    return parse(Int, split(read(Cmd(`sbatch job.sh`; dir = j), String))[end])
end

abort(::Slurm, id::Int) = run(`scancel $id`)

#### HQ
function maybe_scheduler_restart(sc::HQ)
    function readinfo()
        out = Pipe()
        err = Pipe()
        run(pipeline(Cmd(Cmd(string.([split(sc.server_command)..., "server", "info"]));
                         ignorestatus = true); stdout = out, stderr = err))
        close(out.in)
        close(err.in)
        return read(err, String)
    end

    if occursin("No online", readinfo())
        run(Cmd(Cmd(string.([split(sc.server_command)..., "server", "start"]));
                detach = true); wait = false)
        sleep(0.01)
        tries = 0
        while occursin("No online", readinfo()) && tries < 10
            sleep(0.01)
            tries += 1
        end
        if tries == 10
            error("HQ server not reachable")
        end

        maybe_restart_allocs(sc)
        return true
    else
        maybe_restart_allocs(sc)
        return false
    end
end

function maybe_restart_allocs(sc::HQ)
    alloc_lines = readlines(Cmd(Cmd(string.([split(sc.server_command)..., "alloc", "list"]))))
    if length(alloc_lines) == 3 # no allocs -> add all
        allocs_to_add = sc.allocs
    else
        alloc_args = map(a -> replace(strip(split(a, "|")[end-1]), "," => " "),
                         alloc_lines[4:end-1])
        allocs_to_add = filter(a -> !any(x -> x == strip(split(a, "-- ")[end]), alloc_args),
                               sc.allocs)
    end

    for ac in allocs_to_add
        try
            run(Cmd(string.([split(sc.server_command)..., "alloc", "add", split(ac)...])))
        catch e
            log_error(e)
        end
    end
end

function queue(sc::HQ)
    all_lines = readlines(Cmd(string.([split(sc.server_command)..., "job", "list"])))

    start_id = findnext(x -> x[1] == '+', all_lines, 2)
    endid = findnext(x -> x[1] == '+', all_lines, start_id + 1)
    if endid === nothing
        return Dict()
    end
    qlines = all_lines[start_id+1:endid-1]

    jobinfos = [(s = split(x); (parse(Int, s[2]), jobstate(sc, s[6]))) for x in qlines]

    workdir_line_id = findfirst(x -> occursin("Working directory", x),
                                readlines(Cmd(string.([split(sc.server_command)..., "job",
                                                       "info", "$(jobinfos[1][1])"]))))

    function workdir(id)
        return split(readlines(Cmd(string.([split(sc.server_command)..., "job", "info",
                                            "$id"])))[workdir_line_id])[end-1]
    end

    return Dict([workdir(x[1]) => x for x in jobinfos])
end

function jobstate(s::HQ, id::Int)
    lines = readlines(Cmd(string.([split(s.server_command)..., "job", "info", "$id"])))

    if length(lines) <= 1
        return Unknown
    end
    return jobstate(s, split(lines[findfirst(x -> occursin("State", x), lines)])[4])
end

function jobstate(::HQ, state::AbstractString)
    if state == "WAITING"
        return Pending
    elseif state == "RUNNING"
        return Running
    elseif state == "FINISHED"
        return Completed
    elseif state == "CANCELED"
        return Cancelled
    elseif state == "FAILED"
        return Failed
    end
    return Unknown
end

function submit(h::HQ, j::AbstractString)
    chmod(joinpath(j, "job.sh"), 0o777)

    out = read(Cmd(Cmd(string.([split(h.server_command)..., "submit", "./job.sh"]));
                   dir = j), String)
    if !occursin("successfully", out)
        error("Submission error for job in dir $j.")
    end
    return parse(Int, split(out)[end])
end

function abort(h::HQ, id::Int)
    return run(Cmd(string.([split(h.server_command)..., "job", "cancel", "$id"])))
end
