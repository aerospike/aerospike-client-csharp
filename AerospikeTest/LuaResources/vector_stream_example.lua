-- AU-03: stream UDF over records with a vector bin.
-- Client-side Lua receives the vector as bytes (same shape as HLL).
function return_vector_bin(stream, binname)
	local function mapper(rec)
		return rec[binname]
	end
	return stream : map(mapper)
end
