/* 
 * Copyright 2012-2019 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Aerospike exceptions that can be thrown from the client.
	/// </summary>
	public class AerospikeException : Exception
	{
		protected Node node;
		protected Policy policy;
		protected int resultCode = ResultCode.CLIENT_ERROR;
		protected int iteration = -1;
		protected bool inDoubt;

        public AerospikeException(int resultCode, string message) 
            : base(message)
		{
            this.resultCode = resultCode;
		}

        public AerospikeException(int resultCode, Exception e) 
            : base(e.Message, e)
		{
            this.resultCode = resultCode;
		}

        public AerospikeException(int resultCode) 
            : base("")
		{
            this.resultCode = resultCode;
		}

		public AerospikeException(int resultCode, bool inDoubt)
			: base("")
		{
			this.resultCode = resultCode;
			this.inDoubt = inDoubt;
		}

		public AerospikeException(string message, Exception e) 
            : base(message, e)
		{
		}

		public AerospikeException(string message) 
            : base(message)
		{
		}

        public AerospikeException(Exception e)
            : base(e.Message, e)
		{
		}

		/// <summary>
		/// Return error message string.
		/// </summary>
		public override string Message
		{
			get
			{
				StringBuilder sb = new StringBuilder(512);
				string message = base.Message;

				sb.Append("Error ");
				sb.Append(resultCode);

				if (iteration >= 0)
				{
					sb.Append(',');
					sb.Append(iteration);
				}

				if (policy != null)
				{
					sb.Append(',');
					sb.Append(policy.socketTimeout);
					sb.Append(',');
					sb.Append(policy.totalTimeout);
					sb.Append(',');
					sb.Append(policy.maxRetries);
				}

				if (inDoubt)
				{
					sb.Append(",inDoubt");
				}

				if (node != null)
				{
					sb.Append(',');
					sb.Append(node.ToString());
				}

				sb.Append(": ");

				if (message != null && message.Length > 0)
				{
					sb.Append(message);
				}
				else
				{
					sb.Append(ResultCode.GetResultString(resultCode));
				}
				return sb.ToString();
			}
		}

		/// <summary>
		/// Should connection be put back into pool.
		/// </summary>
		public bool KeepConnection()
		{
			return ResultCode.KeepConnection(resultCode);
		}

		/// <summary>
		/// Last node used.
		/// </summary>
		public Node Node
		{
			get
			{
				return node;
			}
			set
			{
				node = value;
			}
		}

		/// <summary>
		/// Transaction policy.
		/// </summary>
		public Policy Policy
		{
			get
			{
				return policy;
			}
			set
			{
				policy = value;
			}
		}

		/// <summary>
		/// Get integer result code.
		/// </summary>
		public int Result
		{
			get
			{
				return resultCode;
			}
		}

		/// <summary>
		/// Number of attempts before failing.
		/// </summary>
		public int Iteration
		{
			get
			{
				return iteration;
			}
			set
			{
				iteration = value;
			}
		}

		/// <summary>
		/// Is it possible that write transaction may have completed.
		/// </summary>
		public bool InDoubt
		{
			get
			{
				return inDoubt;
			}
		}

		/// <summary>
		/// Set whether it is possible that the write transaction may have completed
		/// even though this exception was generated.  This may be the case when a 
		/// client error occurs (like timeout) after the command was sent to the server.
		/// </summary>
		internal void SetInDoubt(bool isRead, int commandSentCounter)
		{
			if (!isRead && (commandSentCounter > 1 || (commandSentCounter == 1 && (resultCode == ResultCode.TIMEOUT || resultCode <= 0))))
			{
				this.inDoubt = true;
			}
		}

		/// <summary>
		/// Exception thrown when database request expires before completing.
		/// </summary>
		public sealed class Timeout : AerospikeException
		{
			/// <summary>
			/// Socket idle timeout in milliseconds.
			/// </summary>
			public int socketTimeout;

			/// <summary>
			/// Total timeout in milliseconds.
			/// </summary>
			public int totalTimeout;

			/// <summary>
			/// If true, client initiated timeout.  If false, server initiated the timeout.
			/// </summary>
			public bool client;

			/// <summary>
			/// Create timeout exception.
			/// </summary>
			public Timeout(int totalTimeout, bool inDoubt)
				: base(ResultCode.TIMEOUT, inDoubt)
			{
				this.socketTimeout = 0;
				this.totalTimeout = totalTimeout;
				this.client = true;
			}

			/// <summary>
			/// Create timeout exception with statistics.
			/// </summary>
			public Timeout(Policy policy, bool client)
				: base(ResultCode.TIMEOUT)
			{
				this.socketTimeout = policy.socketTimeout;
				this.totalTimeout = policy.totalTimeout;
				this.client = client;
			}

			/// <summary>
			/// Get timeout message with statistics.
			/// </summary>
			public override string Message
			{
				get
				{
					if (iteration == -1)
					{
						return "Client timeout: " + totalTimeout;
					}

					StringBuilder sb = new StringBuilder(512);

					if (client)
					{
						sb.Append("Client");
					}
					else
					{
						sb.Append("Server");
					}
					sb.Append(" timeout:");
					sb.Append(" iteration=");
					sb.Append(iteration);
					sb.Append(" socket=");
					sb.Append(socketTimeout);
					sb.Append(" total=");
					sb.Append(totalTimeout);

					if (policy != null)
					{
						sb.Append(" maxRetries=");
						sb.Append(policy.maxRetries);
					}
					sb.Append(" node=");
					sb.Append(node);
					sb.Append(" inDoubt=");
					sb.Append(inDoubt);
					return sb.ToString();
				}
			}
		}

		/// <summary>
		/// Exception thrown when Java serialization error occurs.
		/// </summary>
		public sealed class Serialize : AerospikeException
		{
			/// <summary>
			/// Create serialize exception.
			/// </summary>
			public Serialize(Exception e) : base(ResultCode.SERIALIZE_ERROR, e)
			{
			}

			/// <summary>
			/// Create serialize exception with additional string message.
			/// </summary>
			public Serialize(string message)
				: base(ResultCode.SERIALIZE_ERROR, message)
			{
			}
		}

		/// <summary>
		/// Exception thrown when client can't parse data returned from server.
		/// </summary>
		public sealed class Parse : AerospikeException
		{
			/// <summary>
			/// Create parse exception.
			/// </summary>
			public Parse(string message) : base(ResultCode.PARSE_ERROR, message)
			{
			}
		}

		/// <summary>
		/// Exception thrown when client can't connect to the server.
		/// </summary>
		public sealed class Connection : AerospikeException
		{
			/// <summary>
			/// Create connection exception with string message.
			/// </summary>
			public Connection(string message)
				: base(ResultCode.SERVER_NOT_AVAILABLE, message)
			{
			}

			/// <summary>
			/// Create connection exception with underlying exception.
			/// </summary>
			public Connection(Exception e)
				: base(ResultCode.SERVER_NOT_AVAILABLE, e)
			{
			}

			/// <summary>
			/// Create connection exception with resultCode and message.
			/// </summary>
			public Connection(int resultCode, string message)
				: base(resultCode, message)
			{
			}
		}

		/// <summary>
		/// Exception thrown when chosen node is not active.
		/// </summary>
		public sealed class InvalidNode : AerospikeException
		{
			/// <summary>
			/// Create invalid node exception.
			/// </summary>
			public InvalidNode(int clusterSize, Partition partition)
				: base(ResultCode.INVALID_NODE_ERROR,
					(clusterSize == 0) ? "Cluster is empty" : "Node not found for partition " + partition)
			{
			}

			/// <summary>
			/// Create invalid node exception.
			/// </summary>
			public InvalidNode(String message)
				: base(ResultCode.INVALID_NODE_ERROR, message)
			{
			}
		}

		/// <summary>
		/// Exception thrown when namespace is invalid.
		/// </summary>
		public sealed class InvalidNamespace : AerospikeException
		{
			/// <summary>
			/// Create invalid namespace exception.
			/// </summary>
			public InvalidNamespace(String ns, int mapSize)
				: base(ResultCode.INVALID_NAMESPACE,
					(mapSize == 0) ? "Partition map empty" : "Namespace not found in partition map: " + ns)
			{
			}
		}

		/// <summary>
		/// Exception thrown when scan was terminated prematurely.
		/// </summary>
		public sealed class ScanTerminated : AerospikeException
		{
			/// <summary>
			/// Create scan terminated exception.
			/// </summary>
			public ScanTerminated()
				: base(ResultCode.SCAN_TERMINATED)
			{
			}

			/// <summary>
			/// Create scan terminated exception with underlying exception.
			/// </summary>
			public ScanTerminated(Exception e)
				: base(ResultCode.SCAN_TERMINATED, e)
			{
			}
		}

		/// <summary>
		/// Exception thrown when query was terminated prematurely.
		/// </summary>
		public sealed class QueryTerminated : AerospikeException
		{
			/// <summary>
			/// Create query terminated exception.
			/// </summary>
			public QueryTerminated()
				: base(ResultCode.QUERY_TERMINATED)
			{
			}

			/// <summary>
			/// Create query terminated exception with underlying exception.
			/// </summary>
			public QueryTerminated(Exception e)
				: base(ResultCode.QUERY_TERMINATED, e)
			{
			}
		}

		/// <summary>
		/// Exception thrown when asynchronous command was rejected because the 
		/// max concurrent database commands have been exceeded.
		/// </summary>
		public sealed class CommandRejected : AerospikeException
		{
			/// <summary>
			/// Create command rejected exception.
			/// </summary>
			public CommandRejected() : base(ResultCode.COMMAND_REJECTED)
			{
			}
		}
	}
}
