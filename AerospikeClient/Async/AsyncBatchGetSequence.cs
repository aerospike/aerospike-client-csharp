/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncBatchGetSequence : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly HashSet<string> binNames;
		private readonly RecordSequenceListener listener;
		private readonly int readAttr;

		public AsyncBatchGetSequence
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			HashSet<string> binNames,
			RecordSequenceListener listener,
			int readAttr
		) : base(parent, cluster, node, false)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.binNames = binNames;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchGet(batchNamespace, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord();
			listener.OnRecord(key, record);
		}
	}
}
