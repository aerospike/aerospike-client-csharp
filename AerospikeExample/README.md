# Runnable Documentation Examples

`AerospikeExample` is the execution harness for documentation examples. Example files stay fully compiled and runnable in CI, but the harness owns connection setup, policy setup, logging, result reporting, and server-specific metadata.

The goal is for each example file to stay small enough to be included directly in documentation. The `@@@SNIPSTART` / `@@@SNIPEND` markers carve out smaller portions for tools that splice snippets into markdown files in `aerospike-websites`.

## Example Shape

Use a parameterless example class derived from `SyncExample` or `AsyncExample`:

```csharp
public sealed class PutGet : SyncExample
{
    public override void RunExample()
    {
        Key key = new(ns, set, "putgetkey");
        Bin bin = new("bin1", "value1");

        client.Put(writePolicy, key, bin);
        Record record = client.Get(policy, key);
    }
}
```

The harness injects `console`, `client`, `args`, `ns`, `set`, `policy`, `writePolicy`, and `batchPolicy` at runtime. Avoid adding constructors, local connection setup, seed data, cleanup, or validation to examples unless the example is specifically demonstrating that behavior.

For server-capability gates use the parameterless `Require*` helpers in the base class (`RequireEnterprise`, `RequireMinServerVersion`, `RequireStrongConsistency`, `RequireBasic`, `RequireAuth`, `RequireTls`, `RequirePki`). They throw `ExampleSkipException` so the harness can mark the example as skipped instead of failed.

## Fixtures

`ExampleFixture` carries optional `Setup`, `Validate`, and `Cleanup` actions. Register an example with its fixture through the `ExampleRegistry`:

```csharp
Sync<PutGet>(ValidateAndCleanup<PutGet>(ExampleStateValidation.PutGet, "putgetkey"))
```

Fixtures can set up records before the example runs, validate the behavior afterward, and clean up test data. `Cleanup` runs even when the example throws. This keeps the example file focused on the code that should appear in documentation while still allowing CI to prove the code works.

Common cleanup recipes (`Cleanup<T>(...)`, `ValidateAndCleanup<T>(...)`, `QueryCleanup<T>(...)`, `ScanDefaultSetFixture<T>()`, etc.) live next to the registry entries to keep one-off lambdas to a minimum. Reach for `Fixture<T>(setup, validate, cleanup)` when no recipe applies.

Shared fixture helpers live in `Fixtures/`:

- `ExampleFixtureSupport` — key construction, assertions, bulk put/delete, index and UDF management, and the CDT-aware equality helpers used by validations.
- `ExampleStateValidation` — per-example database-state assertions invoked from the registry.
- `QueryExampleFixtures` / `GetFixture` / `PathExpressionFixture` / `UserDefinedFunctionFixture` / `AsyncUserDefinedFunctionFixture` — seed data for specific examples.

Validation should check observable outcomes such as records, bins, indexes, UDF results, or errors that should exist after the example runs. Some repetition with example code is unavoidable when the only way to observe an outcome is another client call. In those cases, keep the repeated call in the fixture so docs readers still see one clean source example.

## Adding an Example

1. Add a class derived from `SyncExample` or `AsyncExample` next to the existing examples.
2. Implement the parameterless `RunExample` using `console`, `client`, `ns`, `set`, and the harness policies.
3. Register the class in `ExampleRegistry.Examples` using `Sync<T>()` or `Async<T>()`, with the fixture it needs.
4. Add a fixture method when the example needs seed data, validation, cleanup, or server requirements that should not appear in docs.
5. Confirm the example builds and can be run through the standard example runner.

## Running

From the repository root:

```bash
dotnet build Aerospike.sln --configuration Release
dotnet run --project AerospikeExample -- --settings .runsettings PutGet
```

`all` runs every registered example. Examples are routed to the synchronous or asynchronous runner automatically based on the registry.
