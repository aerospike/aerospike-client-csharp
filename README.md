Aerospike C# Client Package
===========================

## AI coding agent entry point

The Aerospike C# client — NuGet package `Aerospike.Client`, namespace
`Aerospike.Client`. Authoritative version: `<Version>` in
`AerospikeClient/AerospikeClient.csproj`. Requires .NET 8+.
API reference: https://aerospike.com/apidocs/csharp/

### What to read, by task

| Task | Read first | Authoritative for |
|---|---|---|
| First read/write | `AerospikeExample/PutGet.cs`, `Get.cs` | what one call does |
| Connection, pooling, TLS, auth | `AerospikeClient/Policy/ClientPolicy.cs`, `TlsPolicy.cs` | connection setup |
| Dynamic client configuration | `AerospikeClient/Config/`, `aerospikeconfig.yaml` | config schema |
| Policies, timeouts, retries | `AerospikeClient/Policy/` | parameter semantics |
| Single-record and `Operate()` | `AerospikeExample/Operate.cs`, `OperateList.cs` | operation composition |
| Batch commands | `AerospikeExample/Batch.cs`, `BatchOperate.cs`, `AerospikeTest/Sync/Basic/TestBatch.cs` | behavior, edge cases |
| Collection data types | `AerospikeClient/CDT/` | list and map operation set |
| Expressions, path expressions | `AerospikeClient/Exp/`, `AerospikeClient/CDT/CDTOperation.cs` | operation set |
| Queries, secondary indexes | `AerospikeExample/Query*.cs`, `AerospikeTest/Sync/Query/` | behavior |
| Transactions | `AerospikeClient/Main/Txn.cs`, `AerospikeTest/Sync/Basic/TestTxn.cs` | behavior |
| Async, listeners | `AerospikeClient/Async/AsyncClient.cs`, `AerospikeClient/Listener/`, `AerospikeTest/Async/` | behavior |
| C# to Aerospike type mapping | `AerospikeClient/Value/` | supported types |
| Errors and result codes | `AerospikeClient/Main/AerospikeException.cs`, `ResultCode.cs` | codes |
| Full public contract, mocking | `AerospikeClient/Main/IAerospikeClient.cs`, `AerospikeClient/Async/IAsyncClient.cs` | the API surface |
| Idiomatic use at feature scale | SubMilliPost — see below (forthcoming) | when and why |

### Repository map

```
aerospike-client-csharp/
├── README.md                entry point (this file)
├── AGENTS.md                pointer to the section above
├── Aerospike.sln            solution — four projects
├── aerospikeconfig.yaml     sample dynamic client configuration
├── .cursorrules             contributor-facing: code style and conventions
├── docfx.json, toc.yml      API reference build (docfx over the XML doc comments)
├── docs/intro.md            guide landing page for the generated site
├── scripts/                 update-version.ps1 — bumps <Version> in every csproj
├── AerospikeClient/         client library — the API surface
│   ├── Main/                top-level API: AerospikeClient, IAerospikeClient,
│   │                        Key, Bin, Record, Operation, Txn, ResultCode, Batch* types
│   ├── Policy/              read, write, batch, query, scan, txn, TLS policies
│   ├── CDT/                 list and map operations; SelectByPath / ModifyByPath
│   ├── Exp/                 filter and operation expressions, incl. CDTExp path expressions
│   ├── Operation/           bit and HLL operations
│   ├── Query/               queries, statements, filters, partition filters
│   ├── Value/               C# to Aerospike type mapping
│   ├── Async/               AsyncClient, IAsyncClient, async commands
│   ├── Listener/            async callback interfaces
│   ├── Config/              dynamic configuration provider and schema
│   ├── Admin/               user and role management
│   ├── Metrics/             client metrics
│   └── Cluster/, Command/, Task/, Lua/, Util/, BCrypt/   internals
├── AerospikeExample/        ~60 runnable single-purpose examples, run in CI
│   ├── README.md            contributor-facing: harness contract, snippet markers
│   ├── Fixtures/            setup, validation, cleanup for examples
│   └── Example.cs, SyncExample.cs, AsyncExample.cs, ExampleRegistry.cs, Program.cs,
│       Arguments.cs, ExampleOutput.cs, ExampleValueFormatter.cs   harness
├── AerospikeTest/           MSTest suite
│   ├── Sync/Basic/          primary suite — 40 files
│   ├── Sync/Query/          queries and secondary indexes — 18 files
│   └── Async/               async coverage — 11 files
├── AerospikeBenchmarks/     load generator, not an API reference
└── .runsettings             host, port, namespace, TLS and auth parameters for test runs
```

Build the API reference locally with `docfx docfx.json` (output lands in `_site`).

### Canonical reference application

SubMilliPost is the planned canonical reference application — a realistic social
newsletter, not isolated API samples. The repository is not yet public; a C#
implementation will be linked here when it is available.

`AerospikeExample/` shows *what a single API call does*. SubMilliPost will show *when and
why* — record and bin layout, how operations compose into a feature, how client APIs
map onto real access paths.

### Precedence when sources disagree

1. https://aerospike.com/apidocs/csharp/ for signatures and parameter semantics
2. `AerospikeTest/` for actual behavior, including edge cases
3. `AerospikeExample/` for single-call usage
4. aerospike.com/docs for server-side semantics and version gates

SubMilliPost will take precedence over `AerospikeExample/` for idiomatic composition at
feature scale once the repository is public with a C# implementation.

The API reference is generated from the XML doc comments in `AerospikeClient/`, so the
source and the site agree by construction. Anything else contradicting them on a
signature is stale. Report it rather than following it.

### Known traps

* **Do not hand-roll batch fan-out.** Batch commands already switch to the
  single-record path when a node's sub-batch has size 1. Application-level
  special-casing for single-key batches is redundant — use the batch APIs on
  `IAerospikeClient`, not a loop.
* **Do not loop single-record calls where a batch counterpart exists.** `Get`,
  `Delete`, `Operate`, and `Exists` all have batch forms taking `Key[]` or
  `List<BatchRecord>`.
* **`ModifyByPath` can remove matching elements.** Pass `Exp.RemoveResult()` as
  the modification expression to delete elements matched by the path context.
  If the path matches one element, one element is removed; if it matches
  multiple elements, all matches are removed.

### Verifying generated code

```bash
dotnet build Aerospike.sln --configuration Release
dotnet test AerospikeTest --settings .runsettings
```

against a local server, then run the examples with
`dotnet run --project AerospikeExample -- --settings .runsettings all`.
Both run in CI on every change. A passing run proves the client builds and the suite
agrees with the installed server version; it does not prove your application logic is
correct.

### Aerospike agent skills

[aerospike/agent-skills](https://github.com/aerospike/agent-skills) carries
core-database and data-modeling guidance — key design, record sizing, collection
choice, indexing. Complementary to this repo, which is authoritative for the C# API
surface.

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
    * **AerospikeExample**    
        C# client examples console application.

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
