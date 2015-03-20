using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    internal sealed class ExistsArrayListenerAdapter : ListenerAdapter<bool[]>, ExistsArrayListener
    {
        public ExistsArrayListenerAdapter(CancellationToken token)
            : base(token)
        {

        }

        public void OnSuccess(Key[] keys, bool[] exists)
        {
            SetResult(exists);
        }
    }
}
