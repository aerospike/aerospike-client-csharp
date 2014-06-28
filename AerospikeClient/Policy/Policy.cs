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
namespace Aerospike.Client
{
	/// <summary>
	/// Container object for transaction policy attributes used in all database
	/// operation calls.
	/// </summary>
	public class Policy
	{
		/// <summary>
		/// Priority of request relative to other transactions.
		/// Currently, only used for scans.
		/// </summary>
		public Priority priority = Priority.DEFAULT;

		/// <summary>
		/// Transaction timeout in milliseconds.
		/// This timeout is used to set the socket timeout and is also sent to the 
		/// server along with the transaction in the wire protocol.
		/// Default to no timeout (0).
		/// </summary>
		public int timeout;

		/// <summary>
		/// Maximum number of retries before aborting the current transaction.
		/// A retry is attempted when there is a network error other than timeout.  
		/// If maxRetries is exceeded, the abort will occur even if the timeout 
		/// has not yet been exceeded. The default number of retries is 2.
		/// </summary>
		public int maxRetries = 2;

		/// <summary>
		/// Milliseconds to sleep between retries if a transaction fails and the 
		/// timeout was not exceeded. The default sleep between retries is 500 ms.
		/// </summary>
		public int sleepBetweenRetries = 500;

		/// <summary>
		/// Allow read operations to use replicated data partitions instead of master
		/// partition. By default, both read and write operations are directed to the
		/// master partition.
		/// <para>
		/// This variable is currently only used in batch read/exists operations. For 
		/// batch, this variable should only be set to true when the replication factor
		/// is greater than or equal to the number of nodes in the cluster.
		/// </para>
		/// </summary>
		public bool allowProleReads;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public Policy(Policy other)
		{
			this.priority = other.priority;
			this.timeout = other.timeout;
			this.maxRetries = other.maxRetries;
			this.sleepBetweenRetries = other.sleepBetweenRetries;
			this.allowProleReads = other.allowProleReads;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public Policy()
		{
		}
	}
}
