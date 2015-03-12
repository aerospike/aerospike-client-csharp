using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    internal sealed class DeleteListenerAdapter : ListenerAdapter<bool>, DeleteListener
    {
        public DeleteListenerAdapter(CancellationToken token)
            : base(token)
        {

        }

        public void OnSuccess(Key key, bool existed)
        {
            SetResult(existed);
        }
    }

}
