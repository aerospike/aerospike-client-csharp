list = clr.Aerospike.Client.LuaList
map = clr.Aerospike.Client.LuaMap
bytes = clr.Aerospike.Client.LuaBytes

local AerospikeClass = {}
AerospikeClass.__index = AerospikeClass

function AerospikeClass.new()
	local self = setmetatable({},AerospikeClass)
	return self
end
	
function AerospikeClass:log(level, message)
	aero.log(level, message)
end

aerospike = AerospikeClass.new()
