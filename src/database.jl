abstract type Storable end

(::Type{S})(s::S) where {S<:Storable} = s

function storage_name(s::S) where {S<:Storable}
    return hasfield(S, :name) ? s.name : error("Please define a name function for type $S.")
end
function storage_directory(s::S) where {S<:Storable}
    return error("Please define a storage_directory function for type $S.")
end

function storage_path(s::S) where {S<:Storable}
    return joinpath(storage_directory(s), storage_name(s))
end
storage_uri(s::Storable) = URI(path="/storage/", query = Dict("path" => storage_path(s)))
verify(s::Storable) = nothing

configurable_fieldnames(::Type{S}) where {S<:Storable} = fieldnames(S)

# These are the standard functions where things are saved as simple jsons in the
# config_path.
"""
    save([server::Server], e::Environment)
    save([server::Server], e::Exec)
    save([server::Server], s::Server)

Saves an item to the database of `server`. If `server` is not specified the item will be stored in the local database.
"""
function save(s::Storable)
    p = config_path("storage", storage_path(s)) * ".json"
    mkpath(splitdir(p)[1])
    if ispath(p)
        @warn "Overwriting previously existing item at $p."
    end
    verify(s)
    return JSON3.write(p, s)
end
function save(server, s::Storable)
    uri = storage_uri(s)
    return HTTP.post(server, uri, s)
end

"""
    load([server::Server], e::Environment)
    load([server::Server], e::Exec)
    load([server::Server], s::Server)

Loads a previously stored item from the `server`. If `server` is not specified the local item is loaded.
"""
function load(s::S) where {S<:Storable}
    p = config_path("storage", storage_path(s)) * ".json"
    if !ispath(p)
        return map(x->splitext(x)[1], readdir(splitdir(p)[1]))
    end
    return JSON3.read(read(p, String), S)
end
function load(server, s::S) where {S<:Storable}
    uri = storage_uri(s)
    if exists(server, s) # asking for a stored item
        return JSON3.read(JSON3.read(HTTP.get(server, uri).body, String), S)
    else
        res = HTTP.get(server, URI(path="/storage/", query= Dict("path"=> splitdir(HTTP.queryparams(uri)["path"])[1])))
        if !isempty(res.body)
            return JSON3.read(res.body, Vector{String})
        else
            return String[]
        end
    end
end

exists(s::Storable) = ispath(config_path("storage", storage_path(s)) * ".json")
function exists(server, s::Storable)
    uri = storage_uri(s)
    try
        res = HTTP.get(server, URI(path="/storage/", query= Dict("path"=> splitdir(HTTP.queryparams(uri)["path"])[1])))
        if !isempty(res.body)
            possibilities = JSON3.read(res.body, Vector{String})
            return storage_name(s) ∈ possibilities
        else
            return false
        end
    catch
        return false
    end
end

"""
    Base.rm([server::Server], e::Environment)
    Base.rm([server::Server], e::Exec)
    Base.rm([server::Server], s::Server)

Removes an item from the database of `server`. If `server` is not specified the item will removed from the local database.
"""
function Base.rm(s::Storable)
    p = config_path("storage", storage_path(s)) * ".json"
    if !ispath(p)
        error("No item found at $p.")
    end
    return rm(p)
end

function Base.rm(server, s::Storable)
    uri = storage_uri(s)
    return HTTP.put(server, uri)
end
