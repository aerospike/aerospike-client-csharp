Aerospike C# Client for NET Core
================================

**Prerequisites**

* NET Core 2.0 or greater.
* [Optional]Visual Studio 2017 or greater.

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
