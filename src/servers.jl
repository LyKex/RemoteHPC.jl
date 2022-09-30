
const SERVER_DIR = config_path("storage/servers")

"""
    Server(name::String, username::String, domain::String, port::Int, scheduler::Scheduler, mountpoint::String,
           julia_exec::String, root_jobdir::String, local_port::Int, max_concurrent_jobs::Int)
    Server(name::String)

A [`Server`](@ref) represents a remote daemon that has the label `name`. It runs on the server defined by
`username` and `domain`. The requirement is that `ssh` is set up in such a way that `ssh username@domain` is
possible, i.e. ssh-copy-id must have been used to not require passwords while executing `ssh` commands.

The daemon will listen to the `port` for http requests and if `local_port` is specified,
a tunnel will be created to guarantee a connection. This is useful in the case that the login node on the remote
server can change.

Calling [`Server`](@ref) with a single `String` will either load the configuration that was previously saved with that label, or go through an interactive setup of a new server.
"""
@kwdef mutable struct Server <: Storable
    name::String = ""
    username::String = ""
    domain::String = ""
    port::Int = 8080
    scheduler::Scheduler = Bash()
    julia_exec::String = "julia"
    root_jobdir::String = ""
    local_port::Int = 0
    max_concurrent_jobs::Int = 100
    uuid::String = ""
end

storage_directory(::Server) = "servers"

function configure_scheduler(s::Server; interactive=true)
    scheduler = nothing
    if haskey(ENV, "DFC_SCHEDULER")
         
        sched = ENV["DFC_SCHEDULER"]
        if occursin("hq", lowercase(sched))
            cmd = get(ENV, "DFC_SCHEDULER_CMD", "hq")
            return HQ(server_command=cmd)
        elseif lowercase(sched) == "slurm"
            return Slurm()
        else
            error("Scheduler $sched not recognized please set a different DFC_SCHEDULER environment var.")
        end
    end
            
    for t in (HQ(), Slurm())
        scmd = submit_cmd(t)
        if server_command(s, "which $scmd").exitcode == 0
            scheduler = t
            break
        end
    end
    if scheduler !== nothing
        return scheduler
    end
    if interactive && scheduler === nothing
        choice = request("Couldn't identify the scheduler select one: ", RadioMenu(["SLURM", "HQ", "BASH"]))

        if choice == 1
            scheduler = Slurm()
        elseif choice == 2
            scheduler = HQ(server_command = ask_input(String, "HQ command", "hq"))
        elseif choice == 3
            scheduler = Bash()
        else
            return
        end
        return scheduler
    else
        return Bash()
    end
end
    

function configure!(s::Server; interactive=true)
    if interactive
        s.port  = ask_input(Int, "Port", s.port)
    end
    if s.domain == "localhost"
        julia = joinpath(Sys.BINDIR, "julia")
    else
        if interactive
            julia = ask_input(String, "Julia Exec", s.julia_exec)
            while server_command(s.username, s.domain, "which $julia").exitcode != 0
                @warn "$julia, no such file or directory."
                julia = ask_input(String, "Julia Exec")
            end
        else
            julia = "julia"
        end
    end
    s.julia_exec = julia

    # Try auto configuring the scheduler
    scheduler = configure_scheduler(s; interactive=interactive)
    if scheduler === nothing
        return
    end
    s.scheduler = scheduler     
    hdir = server_command(s, "pwd").stdout
    if interactive
        dir = ask_input(String, "Default Jobs directory", hdir)
        if dir != hdir
            while server_command(s, "ls $dir").exitcode != 0
                # @warn "$dir, no such file or directory."
                local_choice = request("No such directory, creating one?", RadioMenu(["yes", "no"]))
                if local_choice == 1
                    result = server_command(s, "mkdir -p $dir")
                    if result.exitcode != 0
                        @warn "Couldn't create $dir, try a different one."
                    end
                else
                    dir = ask_input(String, "Default Jobs directory")
                end
            end
        end
        
        s.root_jobdir = dir
        s.max_concurrent_jobs = ask_input(Int, "Max Concurrent Jobs", s.max_concurrent_jobs)
    else
        s.root_jobdir = hdir
    end

    s.uuid = string(uuid4())
    return s
end

function configure_local_port!(s::Server)
    local_choice = request("Should a local tunnel be created?", RadioMenu(["yes", "no"]))
    local_choice == -1 && return
    if local_choice == 1
        s.local_port = ask_input(Int, "Local port", s.local_port)
    else
        s.local_port = 0
    end
end

"""
    configure_local()

Runs through interactive configuration of the local [`Server`](@ref).
"""
function configure_local(;interactive=true)
    host = gethostname()
    @assert !exists(Server(name=host)) "Local server already configured."
    user = ENV["USER"]
    s = Server(name=host, username=user, domain="localhost")
    configure!(s; interactive=interactive)

    @info "saving server configuration...", s
    save(s)
    if interactive
        start_server = request("Start server?", RadioMenu(["yes", "no"]))
        start_server == -1 && return
        if start_server == 1
            start(s)
        end
    end
    return s
end

function Server(s::String)
    t = Server(name=s)
    if exists(t)
        return load(t)
    end
    # Create new server 
    @info "Creating new Server configuration..."
    if occursin("@", s)
        username, domain = split(s, "@")
        name = ask_input(String, "Please specify the Server's identifying name:")
        if exists(Server(name=name, username=username, domain=domain))
            @warn "A server with $name was already configured and will be overwritten."
        end
    elseif s == "localhost"
        username = ENV["USER"]
        domain = "localhost"
        name = s
    else
        username = ask_input(String, "Username")
        domain = ask_input(String, "Domain")
        name = s
    end
    @info "Trying to pull existing configuration from $username@$domain..."

    server = load_config(username, domain)
    if server !== nothing
        server.name = name
        server.domain = domain
        
        change_config = request("Found remote server configuration:\n$server\nIs this correct?", RadioMenu(["yes", "no"]))
        change_config == -1 && return
        if change_config == 2
            configure!(server)
        end
        configure_local_port!(server)
             
    else
        @info "Couldn't pull server configuration, creating new..."
        server = Server(name=name, domain=domain, username=username)
        configure!(server)
        configure_local_port!(server)
    end
    save(server)
    start_server = request("Start server?", RadioMenu(["yes", "no"]))
    start_server == -1 && return
    if start_server == 1
        start(server)
    end
    return server
end


StructTypes.StructType(::Type{Server}) = StructTypes.Struct()
islocal(s::Server) = s.domain == "localhost"
local_server() = Server(gethostname())

Base.joinpath(s::Server, p...) = joinpath(s.root_jobdir, p...)
Base.ispath(s::Server, p...) = islocal(s) ? ispath(p...) :
    JSON3.read(HTTP.get(s, "/ispath/" * joinpath(p...)).body, Bool)

function Base.symlink(s::Server, p, p2)
    if islocal(s)
        symlink(p, p2)
    else
        HTTP.post(s, "/symlink/", [p, p2])
        return nothing
    end
end

function Base.rm(s::Server, p::String)
    if islocal(s)
        isdir(p) ? rm(p, recursive=true) : rm(p)
    else
        HTTP.post(s, "/rm/" * p)
        return nothing
    end
end
function Base.read(s::Server, path::String, type=nothing)
    if islocal(s)
        return type === nothing ? read(path) : read(path, type)
    else
        resp = HTTP.get(s, "/read/" * path)
        t = JSON3.read(resp.body, Vector{UInt8})
        return type === nothing ? t : type(t)
    end
end
function Base.write(s::Server, path::String, v)
    if islocal(s)
        write(path, v)
    else
        resp = HTTP.post(s, "/write/" * path, Vector{UInt8}(v))
        return JSON3.read(resp.body, Int)
    end
end

parse_config(config) = JSON3.read(config, Server)
read_config(config_file) = parse_config(read(config_file, String))

function load_config(username, domain)
    hostname = gethostname(username, domain)
    if domain == "localhost"
        return parse_config(read(config_path("storage","servers","$hostname.json"),String))
    else
        t = server_command(username, domain, "cat ~/.julia/config/RemoteHPC/$hostname/storage/servers/$hostname.json")
        if t.exitcode != 0
            return nothing
        else
            return parse_config(t.stdout)
        end
    end
end
Base.gethostname(username::String, domain::String) = split(server_command(username, domain, "hostname").stdout)[1]
Base.gethostname(s::Server) = gethostname(s.username, s.domain)
load_config(s::Server) = 
     load_config(s.username, s.domain)

ssh_string(s::Server) = s.username * "@" * s.domain
http_string(s::Server) = s.local_port != 0 ? "http://localhost:$(s.local_port)" : "http://$(s.domain):$(s.port)"

function HTTP.request(method::String, s::Server, url, body; kwargs...)
    header = ["Type" => replace("$(typeof(body))", "RemoteHPC."=>""), "USER-UUID" => s.uuid]
    return HTTP.request(method, string(http_string(s), url), header, JSON3.write(body); kwargs...)
end

function HTTP.request(method::String, s::Server, url, body::Vector{UInt8}; kwargs...)
    header = ["Type" => "$(typeof(body))", "USER-UUID" => s.uuid]
    return HTTP.request(method, string(http_string(s), url), header, body; kwargs...)
end

function HTTP.request(method::String, s::Server, url; connect_timeout=1, retries=2, kwargs...)
    header = ["USER-UUID" => s.uuid]
    
    return HTTP.request(method, string(http_string(s), url), header; connect_timeout=connect_timeout, retries=retries, kwargs...)
end

for f in (:get, :put, :post, :head, :patch)
    str = uppercase(string(f))
    @eval function HTTP.$(f)(s::Server, url::AbstractString, args...; kwargs...)
        return HTTP.request("$($str)", s, url, args...; kwargs...)
    end
end

function Base.rm(s::Server)
    return ispath(joinpath(SERVER_DIR, s.name * ".json")) &&
           rm(joinpath(SERVER_DIR, s.name * ".json"))
end

find_tunnel(s) =
    getfirst(x->occursin("ssh -N -f -L $(s.local_port)", x), split(read(pipeline(`ps aux` , stdout = `grep $(s.local_port)`), String), "\n"))

function destroy_tunnel(s)
    t = find_tunnel(s)
    if t !== nothing
        try
            run(`kill $(split(t)[2])`)
        catch
            nothing
        end
    end
end

function construct_tunnel(s)
    run(Cmd(`ssh -N -f -L $(s.local_port):localhost:$(s.port) $(ssh_string(s))`, detach=true))
end

function ask_input(::Type{T}, message, default=nothing) where {T}
    if default === nothing
        t = ""
        print(message * ": ")
        while isempty(t)
            t = readline()
        end
    else
        print(message * " (default: $default): ")
        t = readline()
        if isempty(t)
            return default
        end
    end
    if T != String
        return parse(T, t)
    else
        return t
    end
end
   
"""
    pull(server::Server, remote::String, loc::String)

Pulls `remote` from the server to `loc`.
"""
function pull(server::Server, remote::String, loc::String)
    path = isdir(loc) ? joinpath(loc, splitpath(remote)[end]) : loc
    if islocal(server)
        cp(remote, path; force = true)
    else
        out = Pipe()
        err = Pipe()
        run(pipeline(`scp -r $(ssh_string(server) * ":" * remote) $path`, stdout=out, stderr=err))
        close(out.in)
        close(err.in)
        stderr = read(err, String)
        if !isempty(stderr)
            error("$stderr")
        end
    end
    return path
end

"""
    push(local_file::String, server::Server, server_file::String)

Pushes the `local_file` to the `server_file` on the server.
"""
function push(filename::String, server::Server, server_file::String)
    if islocal(server)
        cp(filename, server_file; force = true)
    else
        out = Pipe()
        err = Pipe()
        run(pipeline(`scp $filename $(ssh_string(server) * ":" * server_file)`, stdout=out, stderr=err))
        close(out.in)
        close(err.in)
    end
end

"Executes a command through `ssh`."
function server_command(username, domain, cmd::String)
    out = Pipe()
    err = Pipe()
    if domain == "localhost"
        process = run(pipeline(ignorestatus(Cmd(string.(split(cmd)))), stdout=out, stderr=err))
    else
        process = run(pipeline(ignorestatus(Cmd(["ssh", "$(username * "@" * domain)",  string.(split(cmd))...])), stdout=out, stderr=err))
    end
    close(out.in)
    close(err.in)

    stdout = read(out, String)
    stderr = read(err, String)
    return (
      stdout = stdout,
      stderr = stderr,
      exitcode = process.exitcode
    )
end
    
server_command(s::Server, cmd) = server_command(s.username, s.domain, cmd)

function has_modules(s::Server)
    try 
        server_command(s, "module avail").code == 0
    catch
        false
    end
end

function available_modules(s::Server)
    if has_modules(s)
        return server_command(s, "module avail")
    else
        return String[]
    end
end

function Base.readdir(s::Server, dir::String)
    resp = HTTP.get(s, "/readdir/" * abspath(s, dir))
    return JSON3.read(resp.body, Vector{String})
end

Base.abspath(s::Server, p) =
    isabspath(p) ? p : joinpath(s, p)

function Base.mtime(s::Server, p)
    if islocal(s)
        return mtime(p)
    else
        resp = HTTP.get(s, "/mtime/" * p)
        return JSON3.read(resp.body, Float64)
    end
end

function Base.filesize(s::Server, p)
    if islocal(s)
        return filesize(p)
    else
        resp = HTTP.get(s, "/filesize/" * p)
        return JSON3.read(resp.body, Float64)
    end
end
