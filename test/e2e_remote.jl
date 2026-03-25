# End-to-end integration test for remote server install, push, and pull.
#
# Required env vars (defaults work for the hostinger test server):
#   REMOTEHPC_E2E_HOST  - SSH alias or hostname  (default: "testserver")
#   REMOTEHPC_E2E_USER  - SSH username            (default: "root")
#   REMOTEHPC_E2E_JULIA - julia path on remote    (default: "julia")
#
# The test assumes passwordless SSH access is configured (e.g. via ~/.ssh/config).
# Run standalone:
#   julia --project=. test/e2e_remote.jl
# Run as part of the full suite:
#   julia --project=. test/runtests.jl

if !isdefined(Main, :RemoteHPC)
    using Test
    using RemoteHPC
    using RemoteHPC: push, pull
end

const E2E_HOST  = get(ENV, "REMOTEHPC_E2E_HOST",  "testserver")
const E2E_USER  = get(ENV, "REMOTEHPC_E2E_USER",  "root")
const E2E_JULIA = get(ENV, "REMOTEHPC_E2E_JULIA", "julia")

# Verify SSH reachability before attempting anything
ssh_ok = try
    RemoteHPC.server_command(E2E_USER, E2E_HOST, "echo ok").stdout |> strip == "ok"
catch
    false
end

if !ssh_ok
    @warn "Skipping remote e2e tests: SSH to $E2E_USER@$E2E_HOST failed"
else

@testset "remote server e2e ($E2E_USER@$E2E_HOST)" begin

    # local server must be alive before starting a remote server
    start(local_server())

    remote = Server(;
        name       = "e2e_remote",
        username   = E2E_USER,
        domain     = E2E_HOST,
        julia_exec = E2E_JULIA,
    )

    # Track remote tmpdir so cleanup can always run
    remote_tmpdir = Ref("")

    @testset "install and start" begin
        remote = RemoteHPC.configure!(remote; interactive = false)
        @test remote !== nothing
        # Install from the dev branch so we test current changes, not the registry version
        RemoteHPC.install_latest(remote)
        save(remote)
        remote = start(remote)
        @test isalive(remote)
        remote_tmpdir[] = strip(RemoteHPC.server_command(remote, "mktemp -d").stdout)
        @test !isempty(remote_tmpdir[])
    end

    if isalive(remote)
        tdir = remote_tmpdir[]

        @testset "push/pull small file (HTTP)" begin
            content = "hello from e2e test\n" * "x"^1000 * "\n"
            local_src = tempname()
            write(local_src, content)
            remote_dst = "$tdir/small.txt"

            push(local_src, remote, remote_dst)
            @test ispath(remote, remote_dst)

            local_out = tempname()
            pull(remote, remote_dst, local_out)
            @test read(local_out, String) == content

            rm(local_src)
            rm(local_out)
        end

        @testset "push/pull binary file (HTTP)" begin
            data = rand(UInt8, 1024)
            local_src = tempname()
            write(local_src, data)
            remote_dst = "$tdir/binary.bin"

            push(local_src, remote, remote_dst)
            @test ispath(remote, remote_dst)

            local_out = tempname()
            pull(remote, remote_dst, local_out)
            @test read(local_out) == data

            rm(local_src)
            rm(local_out)
        end

        @testset "push/pull large file (rsync)" begin
            # 101 MB — just over the 100e6 threshold, forces rsync in both push and pull
            data = zeros(UInt8, 101_000_000)
            local_src = tempname()
            write(local_src, data)
            remote_dst = "$tdir/large.bin"

            push(local_src, remote, remote_dst)
            @test ispath(remote, remote_dst)
            @test filesize(remote, remote_dst) == length(data)

            local_out = tempname()
            pull(remote, remote_dst, local_out)
            @test filesize(local_out) == length(data)
            @test read(local_out) == data

            rm(local_src)
            rm(local_out)
        end

        @testset "push/pull directory (rsync)" begin
            local_dir = mktempdir()
            write(joinpath(local_dir, "a.txt"), "file a")
            write(joinpath(local_dir, "b.txt"), "file b")
            remote_dst = "$tdir/testdir"

            push(local_dir, remote, remote_dst)
            @test isdir(remote, remote_dst)
            @test ispath(remote, "$remote_dst/a.txt")
            @test ispath(remote, "$remote_dst/b.txt")

            local_out = mktempdir()
            pull(remote, remote_dst, local_out)
            # pull places the directory under local_out/testdir
            pulled = joinpath(local_out, "testdir")
            @test isdir(pulled)
            @test read(joinpath(pulled, "a.txt"), String) == "file a"
            @test read(joinpath(pulled, "b.txt"), String) == "file b"

            rm(local_dir; recursive = true)
            rm(local_out; recursive = true)
        end

        @testset "pull non-existent file errors" begin
            @test_throws Exception pull(remote, "$tdir/no_such_file.txt", tempname())
        end
    end

    # Cleanup — runs regardless of test outcomes
    try
        if !isempty(remote_tmpdir[])
            RemoteHPC.server_command(remote, "rm -rf $(remote_tmpdir[])")
        end
        if isalive(remote)
            kill(remote)
        end
        rm(remote)
    catch e
        @warn "Cleanup failed" exception=e
    end

end # @testset

end # if ssh_ok
