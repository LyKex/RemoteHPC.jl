function flagstring(f, v)
    out = ""
    eq_sign = false
    if f[1] != '-'
        if length(f) == 1
            out *= "-$f "
        else
            out *= "--$f"
            eq_sign = true
        end
    else
        if f[1:2] == "--"
            out *= "$f"
            eq_sign = true
        else
            out *= "$f "
        end
    end
    if v !== nothing && !isempty(v)
        if eq_sign
            out *= "="
        end
        if !(v isa AbstractString) && length(v) > 1
            for v_ in v
                out *= "$v_ "
            end
        else
            out *= "$v "
        end
    end
    return out
end

function Base.write(io::IO, e::Exec)
    write(io, "$(e.path) ")
    for (f, v) in e.flags
        write(io, flagstring(f, v))
    end
    write(io, " ")
end

function Base.write(io::IO, c::Calculation)
    write(io, c.exec)
    return write(io, "$(c.args)")
end

function write_exports_string(io::IO, e::Environment)
    for (f, v) in e.exports
        write(io, "export $f=$v\n")
    end
end

function write_preamble_string(io::IO, e::Environment, sched::Scheduler)
    for (f, v) in e.directives
        write(io, "$(directive_prefix(sched)) $(flagstring(f, v))\n")
    end
    return write(io, "$(e.preamble)\n")
end

write_postamble_string(io::IO, e::Environment) = write(io, "$(e.postamble)\n")

function Base.write(io::IO, job_info::Tuple, sched::Scheduler)
    name, e, calcs = job_info

    write(io, "#!/bin/bash\n")
    write(io, "# Generated by RemoteHPC\n")
    write(io, "$(name_directive(sched)) $name\n")

    write_preamble_string(io, e, sched)
    write_exports_string(io, e)

    modules = String[]
    for c in calcs
        for m in c.exec.modules
            if !(m ∈ modules)
                push!(modules, m)
            end
        end
    end
    # break into different lines to make sure modules are loaded sequentially
    map(modules) do m
        write(io, "module load $(m)\n")
    end

    for c in calcs
        #TODO: Put all of this in write(io, c)
        write(io, c.run ? "" : "#")
        if c.exec.parallel
            write(io, e.parallel_exec)
            names = "$(e.parallel_exec.name) $(c.exec.name)"
        else
            names = c.exec.name
        end
        write(io, c)
        write(io, "\n")
    end

    return write_postamble_string(io, e)
end
