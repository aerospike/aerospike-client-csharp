using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    internal sealed class ExistsListenerAdapter : ListenerAdapter<bool>, ExistsListener
    {
        public ExistsListenerAdapter(CancellationToken token)
            : base(token)
        {

        }

        public void OnSuccess(Key key, bool exists)
        {
            SetResult(exists);
        }

    }
}
