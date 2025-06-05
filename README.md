Aerospike C# Client Package
===========================

**Prerequisites**

* .NET 8+
* [Optional]Visual Studio 2022+

**Contents**

* **Aerospike.sln**    
    Visual Studio solution for C# client. The projects are:
    
    * **AerospikeClient**    
        C# client library.
    * **AerospikeTest**    
        C# client unit tests.
    * **AerospikeBenchmarks**    
        C# client benchmarks command line application.
    * **AerospikeDemo**    
        C# client examples WinForms application. Windows only.
    * **AerospikeAdmin**    
        Aerospike user administration WinForms application. Windows only. This application is only valid for enterprise servers that are configured for user authentication.

**Windows/Visual Studio Build Instructions**

* Double click on Aerospike.sln.  The solution will be opened in Visual Studio.
* Click menu Build -> Configuration Manager.
* Click desired solution configuration and platform.
* Click Close.
* Click Build -> Build Solution

**Linux/CommandLine Build Instructions**

    $ cd AerospikeClient
    $ dotnet restore
    $ dotnet build --configuration Release
    $ cd ../AerospikeTest
    $ dotnet restore
    $ dotnet build --configuration Release
    $ dotnet test --configuration Release
    $ cd ../AerospikeBenchmarks
    $ dotnet restore
    $ dotnet build --configuration Release

