/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Diagnostics;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class LatencyType
	{
		public const int CONN = 0;
		public const int WRITE = 1;
		public const int READ = 2;
		public const int BATCH = 3;
		public const int QUERY = 4;
		public const int NONE = 5;
	}

	public sealed class Metrics
	{
		private LatencyManager[] latency;
		private int errors;
		private int timeouts;

		public Metrics(MetricsPolicy policy)
        {
			int latencyColumns = policy.latencyColumns;
			int latencyShift = policy.latencyShift;

			latency = new LatencyManager[LatencyType.NONE];
			latency[LatencyType.CONN] = new LatencyManager(latencyColumns, latencyShift);
			latency[LatencyType.WRITE] = new LatencyManager(latencyColumns, latencyShift);
			latency[LatencyType.READ] = new LatencyManager(latencyColumns, latencyShift);
			latency[LatencyType.BATCH] = new LatencyManager(latencyColumns, latencyShift);
			latency[LatencyType.QUERY] = new LatencyManager(latencyColumns, latencyShift);
		}

		public void AddLatency(int type, long elapsed)
		{
			latency[type].Add(elapsed);
		}

		public LatencyManager Get(int type)
		{
			return latency[type];
		}

		public void AddError()
		{
			Interlocked.Increment(ref errors);
		}

		public void AddTimeout()
		{
			Interlocked.Increment(ref timeouts);
		}

		public int ResetError()
		{
			return Interlocked.Exchange(ref errors, 0);
		}

		public int ResetTimeout()
		{
			return Interlocked.Exchange(ref timeouts, 0);
		}
	}

	public sealed class MetricsWriter
    {
		private readonly StringBuilder sb;
		private readonly StreamWriter writer;
		private readonly int latencyColumns;
		private readonly int latencyShift;
		private DateTime beginTime;
		private TimeSpan beginSpan;
		private bool closed;

		public MetricsWriter(MetricsPolicy policy)
        {
			latencyColumns = policy.latencyColumns;
			latencyShift = policy.latencyShift;
			sb = new StringBuilder(512);
			
			FileStream fs = new FileStream(policy.reportPath, FileMode.Append, FileAccess.Write);
			writer = new StreamWriter(fs);
			
			LatencyManager.PrintHeader(sb, latencyColumns, latencyShift);
			writer.WriteLine(sb.ToString());

			beginTime = DateTime.UtcNow;
			beginSpan = Process.GetCurrentProcess().TotalProcessorTime;
		}

		public void WriteCluster(Cluster cluster)
		{
			lock (writer)
			{
				if (!closed)
				{
					Write(cluster);
				}
			}
		}

		public void WriteNode(Node node)
		{
			lock (writer)
			{
				if (!closed)
				{
					sb.Length = 0;
					sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
					Write(node);
					writer.WriteLine();
				}
			}
		}

		public void Close(Cluster cluster)
		{
			lock (writer)
			{
				if (!closed)
				{
					try
					{
						closed = true;
						Write(cluster);
						writer.Close();
					}
					catch (Exception e)
					{
						Log.Error("Failed to close metrics writer: " + Util.GetErrorMessage(e));
					}
				}
			}
		}

		private void Write(Cluster cluster)
		{
			Process proc = Process.GetCurrentProcess();
			long mem = proc.PrivateMemorySize64;
			TimeSpan endSpan = proc.TotalProcessorTime;
			DateTime endTime = DateTime.UtcNow;
			double cpu = ((endSpan - beginSpan).TotalMilliseconds * 100.0) / ((endTime - beginTime).TotalMilliseconds * Environment.ProcessorCount);
			int threadExpandCount = cluster.ResetThreadExpandCount();
			ClusterStats stats = new ClusterStats(null);

			sb.Length = 0;
			sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
			sb.Append(" cluster ");
			sb.Append((int)cpu);
			sb.Append(' ');
			sb.Append(mem);
			sb.Append(' ');
			sb.Append(threadExpandCount);
			sb.Append(' ');
			sb.Append(stats.threadsInUse);
			sb.Append(' ');
			sb.Append(stats.completionPortsInUse);
			writer.Write(sb.ToString());

			Node[] nodes = cluster.Nodes;

			foreach (Node node in nodes)
			{
				sb.Length = 0;
				Write(node);
			}
			writer.WriteLine();

			beginTime = endTime;
			beginSpan = endSpan;
		}

		private void Write(Node node)
		{
			NodeStats ns = new NodeStats(node);
			ConnectionStats cs = ns.syncStats;
			Metrics metrics = node.Metrics;
			int errors = metrics.ResetError();
			int timeouts = metrics.ResetTimeout();

			sb.Append(" node ");
			sb.Append(node);
			sb.Append(' ');
			sb.Append(cs.inUse);
			sb.Append(' ');
			sb.Append(cs.inPool);
			sb.Append(' ');
			sb.Append(cs.opened);
			sb.Append(' ');
			sb.Append(cs.closed);
			sb.Append(' ');
			sb.Append(errors);
			sb.Append(' ');
			sb.Append(timeouts);
			writer.Write(sb.ToString());

			WriteLatency(node, metrics.Get(LatencyType.CONN), "conn");
			WriteLatency(node, metrics.Get(LatencyType.WRITE), "write");
			WriteLatency(node, metrics.Get(LatencyType.READ), "read");
			WriteLatency(node, metrics.Get(LatencyType.BATCH), "batch");
			WriteLatency(node, metrics.Get(LatencyType.QUERY), "query");
		}

		private void WriteLatency(Node node, LatencyManager lm, string type)
		{
			if (lm.PrintResults(node, sb, type))
			{
				writer.Write(sb.ToString());
			}
		}
	}
}
