Aerospike C# Client for NET Framework
=====================================

**Prerequisites**

* Windows 7/Windows Server 2008 or greater, preferably 64 bit.
* Visual Studio 2017 or greater, preferably 64 bit.
* .NET 4.7.2 or greater.

**Contents**

* **Aerospike.sln**    
	C# library and demonstration programs with full functionality.  The projects are:
	
	* **AerospikeClient**    
		C# client library.
	* **AerospikeDemo**    
		C# demonstration program for examples and benchmarks.
	* **AerospikeTest**    
		C# client unit tests.
	* **AerospikeAdmin**    
		Aerospike user administration.  This application is only valid for enterprise servers that are configured to require user authentication.
	
The solution supports the following configurations:

* Debug
* Release
* Debug IIS : Same as Debug, but store reusable buffers in HttpContext.Current.Items.
* Release IIS : Same as Release, but store reusable buffers in HttpContext.Current.Items.


**Build Instructions**

* Double click on Aerospike.sln.  The solution will be opened in Visual Studio.
* Click menu Build -> Configuration Manager.
* Click desired solution configuration and platform.
* Click Close.
* Click Build -> Build Solution

**Demonstration Instructions**

* Ensure Aerospike server has been installed and is operational.
* Open "Aerospike.sln" in Visual Studio.
* Press F5 to run demonstration program.
* Enter server hostname/port of the Aerospike server.
* Enter server's namespace and set name if they are different from the default.
* Click on an example (i.e. ServerInfo).  The example source code will be displayed.
* Press "Start" button.  

The console window will display the log results of the example.
Long running examples can be stopped with "Stop" button.
