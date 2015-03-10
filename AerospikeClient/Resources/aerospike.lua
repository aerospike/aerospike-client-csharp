-- ############################################################################
--
-- LOG FUNCTIONS
--
-- ############################################################################

function trace(m, ...)
    return aerospike:log(4, string.format(m, ...))
end

function debug(m, ...)
    return aerospike:log(3, string.format(m, ...))
end

function info(m, ...)
    return aerospike:log(2, string.format(m, ...))
end

function warn(m, ...)
    return aerospike:log(1, string.format(m, ...))
end

-- ############################################################################
--
-- APPLY FUNCTIONS
--
-- ############################################################################

--
-- Apply function to a record and arguments.
--
-- @param f the fully-qualified name of the function.
-- @param r the record to be applied to the function.
-- @param ... additional arguments to be applied to the function.
-- @return result of the called function or nil.
-- 
function apply_record(f, r, ...)

    if f == nil then
        error("function not found", 2)
    end

    success, result = pcall(f, r, ...)
    if success then
        return result
    else
        error(result, 2)
        return nil
    end
end

--
-- Apply function to an iterator and arguments.
--
-- @param f the fully-qualified name of the function.
-- @param s the iterator to be applied to the function.
-- @param ... additional arguments to be applied to the function.
-- @return 0 on success, otherwise failure.
--
function apply_stream(f, scope, istream, ostream, ...)

    if f == nil then
        error("function not found", 2)
        return 2
    end
    
    require("stream_ops")

    local stream_ops = StreamOps_create();
    
    success, result = pcall(f, stream_ops, ...)

    -- info("apply_stream: success=%s, result=%s", tostring(success), tostring(result))

    if success then

        local ops = StreamOps_select(result.ops, scope);
        
        -- Apply server operations to the stream
        -- result => a stream_ops object
        local values = StreamOps_apply(stream_iterator(istream), ops);

        -- Iterate the stream of values from the computation
        -- then pipe it to the ostream
        for value in values do
            -- info("value = %s", tostring(value))
            stream.write(ostream, value)
        end

        -- 0 is success
        return 0
    else
        error(result, 2)
        return 2
    end
end
