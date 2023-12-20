using System.Diagnostics;

namespace Aerospike.Client
{
    public class Tracing
    {
        private const string SOURCE_NAME = "Aerospike.Client";

        internal const string SERVICE_NAME = "Aerospike";

        internal static readonly ActivitySource Source = new ActivitySource(SOURCE_NAME);
    }
}