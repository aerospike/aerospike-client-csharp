/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for server task completion.
	/// </summary>
	public abstract class Task
	{
		protected internal readonly Cluster cluster;
		private bool done;

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public Task(Cluster cluster, bool done)
		{
			this.cluster = cluster;
			this.done = done;
		}


		/// <summary>
		/// Wait for asynchronous task to complete using default sleep interval.
		/// </summary>
		public void Wait()
		{
			Wait(1000);
		}

		/// <summary>
		/// Wait for asynchronous task to complete using given sleep interval.
		/// </summary>
		public void Wait(int sleepInterval)
		{
			while (!done)
			{
				Util.Sleep(sleepInterval);
				done = IsDone();
			}
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public abstract bool IsDone();
	}
}