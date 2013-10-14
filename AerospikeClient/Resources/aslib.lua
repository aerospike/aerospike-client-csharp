as_metatable = 
{
	__call = function(t,v)
		if v then
			return t.create_set(v)
		else
			return t.create()
		end
	end
}

list = {}
setmetatable(list, as_metatable)

function list.iterator(l)
	local iter = list.create_iterator(l);
	return function()
		return list.next(iter)
	end
end

map = {}
setmetatable(map, as_metatable)

function map.pairs(m)
	local iter = map.create_iterator(m);
	return function()
		if map.next(iter) then
			return map.key(iter), map.value(iter)
		else
			return nil
		end
	end
end

function map.keys(m)
	local iter = map.create_iterator(m);
	return function()
		return map.next_key(iter)
	end
end

function map.values(m)
	local iter = map.create_iterator(m);
	return function()
		return map.next_value(iter)
	end
end

bytes = {}
setmetatable(bytes, as_metatable)

stream = {}
setmetatable(stream, as_metatable)

aero = {}

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
