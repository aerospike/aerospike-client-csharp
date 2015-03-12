using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    internal sealed class RecordArrayListenerAdapter : ListenerAdapter<Record[]>, RecordArrayListener
    {
        public RecordArrayListenerAdapter(CancellationToken token)
            : base(token)
        {

        }

        public void OnSuccess(Key[] keys, Record[] records)
        {
            SetResult(records);
        }
    }
}
