function wait_and_update(rec, bins, n)
    info("WAIT_AND_WRITE BEGIN")
    sleep(n)
    info("WAIT FINISHED")
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    status = aerospike:update(rec)
    return status
end

function rec_create(rec, bins)
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    status = aerospike:create(rec)
    return status
end