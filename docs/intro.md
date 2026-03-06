# Aerospike C# Client

The Aerospike C# client library provides a .NET interface to the Aerospike database.

## Prerequisites

* .NET 8+
* [Optional] Visual Studio 2022+

## Projects

| Project | Description |
|---------|-------------|
| **AerospikeClient** | C# client library |
| **AerospikeTest** | Unit tests |
| **AerospikeBenchmarks** | Benchmarks command line application |
| **AerospikeDemo** | Examples WinForms application (Windows only) |
| **AerospikeAdmin** | User administration WinForms application (Windows only, enterprise servers only) |

## Quick Start

```bash
cd AerospikeClient
dotnet restore
dotnet build --configuration Release
```

See the [API Reference](../api/) for detailed class and method documentation.
