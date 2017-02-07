Aerospike C# Client for NET Core
================================

**Prerequisites**

* NET Core 1.1 or greater.
* [Optional]Visual Studio 2015 Update 3 or greater.

**Contents**

* **Aerospike.sln**    
	Visual Studio solution for NET Core C# client library and unit tests.  The projects are:
	
	* **AerospikeClient**    
		C# client library.
	* **AerospikeTest**    
		C# client unit tests.

**Build Instructions**

	$ cd Core/AerospikeClient
	$ dotnet restore
	$ dotnet build --configuration Release    

**Test Instructions**

	$ cd Core/AerospikeTest
	$ dotnet restore
	$ dotnet build --configuration Release 
	$ dotnet test --configuration Release

**Functionality**

This package supports all C# client functionality except:

* Aggregation queries.  Aggregation queries require Lua code to be executed on the client side and NeoLua does not support NET Core.  The following client methods are not supported:

		public ResultSet QueryAggregate(QueryPolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		public void QueryAggregate(QueryPolicy policy, Statement statement, Action<Object> action)
		public ResultSet QueryAggregate(QueryPolicy policy, Statement statement)
	
* Automatic serialization of C# objects to binary.  NET Core does not support object serialization.  The following bin constructor is not supported:

		public Bin(string name, object value)
	
	Users should serialize these objects to binary using their 3rd party serializer of choice (protobuf, msgpack, etc...) and then use the byte array bin constructor:

		public Bin(string name, byte[] value)
