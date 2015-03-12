using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    internal abstract class ListenerAdapter<T>
    {
        public ListenerAdapter(CancellationToken token)
        {
            token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
        }

        protected void SetResult(T result)
        {
            tcs.TrySetResult(result);
        }

        public void OnFailure(AerospikeException exception)
        {
            tcs.SetException(exception);
        }

        public System.Threading.Tasks.Task<T> Task { get { return tcs.Task; } }

        private readonly TaskCompletionSource<T> tcs = new TaskCompletionSource<T>();
    }
}
