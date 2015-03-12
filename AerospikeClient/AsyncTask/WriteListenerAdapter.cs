using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    internal sealed class WriteListenerAdapter : ListenerAdapter<Key>, WriteListener
    {
        public WriteListenerAdapter(CancellationToken token)
            : base(token)
        {

        }

        public void OnSuccess(Key key)
        {
            SetResult(key);
        }
    }
}
