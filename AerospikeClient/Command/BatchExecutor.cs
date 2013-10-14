/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class BatchExecutor
	{
		private readonly Thread thread;
		private readonly Policy policy;
		private readonly BatchNode batchNode;
		private readonly HashSet<string> binNames;
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly Record[] records;
		private readonly bool[] existsArray;
		private readonly int readAttr;
		private Exception exception;

		public BatchExecutor(Policy policy, BatchNode batchNode, Dictionary<Key, BatchItem> keyMap, HashSet<string> binNames, Record[] records, bool[] existsArray, int readAttr)
		{
			this.policy = policy;
			this.batchNode = batchNode;
			this.keyMap = keyMap;
			this.binNames = binNames;
			this.records = records;
			this.existsArray = existsArray;
			this.readAttr = readAttr;
			this.thread = new Thread(new ThreadStart(this.Run));
		}

		public void Start()
		{
			thread.Start();
		}

		public void Join()
		{
			thread.Join();
		}

		public void Run()
		{
			try
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					if (records != null)
					{
						BatchCommandGet command = new BatchCommandGet(batchNode.node, keyMap, binNames, records);
						command.SetBatchGet(batchNamespace, binNames, readAttr);
						command.Execute(policy);
					}
					else
					{
						BatchCommandExists command = new BatchCommandExists(batchNode.node, keyMap, existsArray);
						command.SetBatchExists(batchNamespace);
						command.Execute(policy);
					}
				}
			}
			catch (Exception e)
			{
				exception = e;
			}
		}

		public Exception Exception
		{
			get
			{
				return exception;
			}
		}

		public static void ExecuteBatch(Cluster cluster, Policy policy, Key[] keys, bool[] existsArray, Record[] records, HashSet<string> binNames, int readAttr)
		{
			Dictionary<Key, BatchItem> keyMap = BatchItem.GenerateMap(keys);
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, keys);

			// Dispatch the work to each node on a different thread.
			List<BatchExecutor> threads = new List<BatchExecutor>(batchNodes.Count);

			foreach (BatchNode batchNode in batchNodes)
			{
				BatchExecutor thread = new BatchExecutor(policy, batchNode, keyMap, binNames, records, existsArray, readAttr);
				threads.Add(thread);
				thread.Start();
			}

			// Wait for all the threads to finish their work and return results.
			foreach (BatchExecutor thread in threads)
			{
				try
				{
					thread.Join();
				}
				catch (Exception)
				{
				}
			}

			// Throw an exception if an error occurred.
			foreach (BatchExecutor thread in threads)
			{
				Exception e = thread.Exception;

				if (e != null)
				{
					if (e is AerospikeException)
					{
						throw (AerospikeException)e;
					}
					else
					{
						throw new AerospikeException(e);
					}
				}
			}
		}
	}
}