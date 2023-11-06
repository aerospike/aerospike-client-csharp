--udf functions for udf testing
function sleep(n) -- seconds
    local t0 = os.time()
    while os.time() - t0 < n do end
end

-- Create a record
function rec_create(rec, bins)
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    status = aerospike:create(rec)
    return status
end

-- Update a record
function rec_update(rec, bins)
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    status = aerospike:update(rec)
    return status
end

function wait_and_update(rec, bins, n, optim)
    info("WAIT_AND_WRITE BEGIN")
    if (optim == 1) then 
        local ffi = require "ffi"
        ffi.cdef "unsigned int sleep(unsigned int seconds);"
        ffi.C.sleep(n)
    else
        sleep(n)
    end
    info("WAIT FINISHED")
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    status = aerospike:update(rec)
    return status
end

function wait_and_create(rec, bins, n, optim)
    info("WAIT_AND_WRITE BEGIN")
    if (optim == 1) then 
        local ffi = require "ffi"
        ffi.cdef "unsigned int sleep(unsigned int seconds);"
        ffi.C.sleep(n)
    else
        sleep(n)
    end
    if n == 20 then
        info("this is 20 sec sleep")
    end
    info("WAIT FINISHED")
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    status = aerospike:create(rec)
    return status
end
-- Delete regardless of record existence
function rec_delete(rec, bin)
    status = aerospike:remove(rec)
    return status
end

function bin_increment(rec, bins)
    if bins ~= nil then
        for b, incval in map.pairs(bins) do
            rec[b] = rec[b]+incval
        end
    end
    status = aerospike:update(rec)
    return status
end

function bin_prepend(rec, bins)
    if bins ~= nil then
        for b, catval in map.pairs(bins) do
            rec[b] = catval..rec[b]
        end
    end
    status = aerospike:update(rec)
    return status
end

function bin_append(rec, bins)
    if bins ~= nil then
        for b, catval in map.pairs(bins) do
            rec[b] = rec[b]..catval
        end
    end
    status = aerospike:update(rec)
    return status
end


-- Check existance of record
function rec_exists(rec)
    return aerospike:exists(rec)
end

function get_binval(rec, bin)
    -- Return bin value of the record
    return rec[bin]
end


function list_insert(rec, bin, ind, value)
    local l = rec[bin]
    list.insert(l, ind, value)
    rec[bin] = l
    status = aerospike:update(rec)
    return status
end

function list_append(rec, bin, value)
    local l = rec[bin]
    list.append(l, value)
    rec[bin] = l
    status = aerospike:update(rec)
    return status
end

function list_insert_items(rec, bin, ind, values)
    local l = rec[bin]
    local bins = values
    for value in list.iterator(bins) do
        list.insert(l, ind, value)
        ind = ind+1
    end
    rec[bin] = l
    status = aerospike:update(rec)
    return status
end


function list_append_items(rec, bin, values)
    local l = rec[bin]
    list.merge(l, values)
    rec[bin] = l
    status = aerospike:update(rec)
    return status
end


function list_bin_incr(rec, bin, ind, val)
    local l = rec[bin]
    local count = 1
    for value in list.iterator(l) do
        if count == ind then
            list.remove(l, value)
            value = value+val
            list.insert(l, ind, value)
        end
        count = count+1
    end
    rec[bin] = l
    status = aerospike:update(rec)
    return status
end
