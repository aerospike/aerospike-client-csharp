/* 
 * Copyright 2012-2024 Aerospike, Inc.
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

using static Aerospike.Client.Latency;
using System.Drawing;
using System.IO;
using System.Text;
using System;
using System.Diagnostics;

namespace Aerospike.Client
{
	/// <summary>
	/// Client metrics listener.
	/// </summary>
	public sealed class MetricsWriter : IMetricsListener
	{
		private static readonly string FilenameFormat = "yyyyMMddHHmmss";
		private static readonly string TimestampFormat = "yyyy-MM-dd HH:mm:ss.SSS";
		private static readonly int MinFileSize = 1000000;

		private readonly string Dir;
		private readonly StringBuilder Sb;
		private StreamWriter Writer;
		private long Size;
		private long MaxSize;
		private int LatencyColumns;
		private int LatencyShift;
		private bool Enabled;
		private DateTime prevTime;
		private TimeSpan prevCpuUsage;

		/// <summary>
		/// Initialize metrics writer.
		/// </summary>
		public MetricsWriter(String dir)
		{
			this.Dir = dir;
			this.Sb = new StringBuilder(8192);
			this.prevTime = DateTime.UtcNow;
			this.prevCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;
		}

		/// <summary>
		/// Open timestamped metrics file in Append mode and write header indicating what metrics will
		/// be stored.
		/// </summary>

		public void OnEnable(Cluster cluster, MetricsPolicy policy)
		{
			if (policy.ReportSizeLimit != 0 && policy.ReportSizeLimit < MinFileSize)
			{
				throw new AerospikeException("MetricsPolicy.reportSizeLimit " + policy.ReportSizeLimit +
					" must be at least " + MinFileSize);
			}

			this.MaxSize = policy.ReportSizeLimit;
			this.LatencyColumns = policy.LatencyColumns;
			this.LatencyShift = policy.LatencyShift;

			try
			{
				Directory.CreateDirectory(Dir);
				Open();
			}
			catch (IOException ioe)
			{
				throw new AerospikeException(ioe);
			}

			Enabled = true;
		}

		/// <summary>
		/// Write cluster metrics snapshot to file.
		/// </summary>
		public void OnSnapshot(Cluster cluster)
		{
			lock (this)
			{
				if (Enabled)
				{
					WriteCluster(cluster);
				}
			}
		}

		/// <summary>
		/// Write final node metrics snapshot on node that will be closed.
		/// </summary>
		public void OnNodeClose(Node node)
		{
			lock (this)
			{
				if (Enabled)
				{
					Sb.Append(DateTime.Now.ToString(TimestampFormat));
					Sb.Append(" node");
					WriteNode(node);
					WriteLine();
				}
			}
		}

		/// <summary>
		/// Write final cluster metrics snapshot to file and then close the file.
		/// </summary>
		public void OnDisable(Cluster cluster)
		{
			lock (this)
			{
				if (Enabled)
				{
					try
					{
						Enabled = false;
						WriteCluster(cluster);
						Writer.Close();
					}
					catch (Exception e)
					{
						Log.Error("Failed to close metrics writer: " + Util.GetErrorMessage(e));
					}
				}
			}
		}

		private void Open()
		{
			DateTime now = DateTime.Now;
			string path = Dir + Path.DirectorySeparatorChar + "metrics-" + now.ToString(FilenameFormat) + ".log";
			Writer = new StreamWriter(path, true);
			Size = 0;

			Sb.Append(now.ToString(TimestampFormat));
			Sb.Append(" header(1)");
			Sb.Append(" cluster[name,cpu,mem,invalidNodeCount,tranCount,retryCount,delayQueueTimeoutCount,asyncThreadsInUse,asyncCompletionPortsInUse,node[]]");
			Sb.Append(" node[name,address,port,syncConn,asyncConn,errors,timeouts,latency[]]");
			Sb.Append(" conn[inUse,inPool,opened,closed]");
			Sb.Append(" latency(");
			Sb.Append(LatencyColumns);
			Sb.Append(',');
			Sb.Append(LatencyShift);
			Sb.Append(')');
			Sb.Append("[type[l1,l2,l3...]]");
			WriteLine();
		}

		private void WriteCluster(Cluster cluster)
		{
			String clusterName = cluster.clusterName;

			if (clusterName == null)
			{
				clusterName = "";
			}

			GetCpuMemoryUsage(out double cpu, out long mem);

			Sb.Append(DateTime.Now.ToString(TimestampFormat));
			Sb.Append(" cluster[");
			Sb.Append(clusterName);
			Sb.Append(',');
			Sb.Append((int)cpu);
			Sb.Append(',');
			Sb.Append(mem);
			Sb.Append(',');
			Sb.Append(cluster.InvalidNodeCount); // Cumulative. Not reset on each interval.
			Sb.Append(',');
			Sb.Append(cluster.GetTranCount());  // Cumulative. Not reset on each interval.
			Sb.Append(',');
			Sb.Append(cluster.GetRetryCount()); // Cumulative. Not reset on each interval.
			Sb.Append(',');
			Sb.Append(cluster.GetDelayQueueTimeoutCount()); // Cumulative. Not reset on each interval.
			Sb.Append(",");

			int workerThreadsMax;
			int completionPortThreadsMax;
			ThreadPool.GetMaxThreads(out workerThreadsMax, out completionPortThreadsMax);

			int workerThreads;
			int completionPortThreads;
			ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);

			var threadsInUse = workerThreadsMax - workerThreads;
			var completionPortsInUse = completionPortThreadsMax - completionPortThreads;

			Sb.Append(threadsInUse);
			Sb.Append(",");
			Sb.Append(completionPortsInUse);
			Sb.Append(",[");

			Node[] nodes = cluster.Nodes;

			for (int i = 0; i < nodes.Length; i++)
			{
				Node node = nodes[i];

				if (i > 0)
				{
					Sb.Append(',');
				}
				WriteNode(node);
			}
			Sb.Append("]]");
			WriteLine();
		}

		private void WriteNode(Node node)
		{
			Sb.Append('[');
			Sb.Append(node.Name);
			Sb.Append(',');

			Host host = node.Host;

			Sb.Append(host.name);
			Sb.Append(',');
			Sb.Append(host.port);
			Sb.Append(',');

			WriteConn(node.GetConnectionStats());
			Sb.Append(',');
			var asyncStats = new ConnectionStats(0, 0, 0, 0);
			if (node is AsyncNode)
			{
				asyncStats = ((AsyncNode)node).GetAsyncConnectionStats();
			}
			WriteConn(asyncStats);
			Sb.Append(',');

			Sb.Append(node.GetErrorCount());   // Cumulative. Not reset on each interval.
			Sb.Append(',');
			Sb.Append(node.GetTimeoutCount()); // Cumulative. Not reset on each interval.
			Sb.Append(",[");

			NodeMetrics nm = node.GetMetrics();
			int max = Latency.GetMax();

			for (int i = 0; i < max; i++)
			{
				if (i > 0)
				{
					Sb.Append(',');
				}

				Sb.Append(LatencyTypeToString((LatencyType)i));
				Sb.Append('[');

				LatencyBuckets buckets = nm.GetLatencyBuckets(i);
				int bucketMax = buckets.GetMax();

				for (int j = 0; j < bucketMax; j++)
				{
					if (j > 0)
					{
						Sb.Append(',');
					}
					Sb.Append(buckets.GetBucket(j)); // Cumulative. Not reset on each interval.
				}
				Sb.Append(']');
			}
			Sb.Append("]]");
		}

		private void WriteConn(ConnectionStats cs)
		{
			Sb.Append(cs.inUse);
			Sb.Append(',');
			Sb.Append(cs.inPool);
			Sb.Append(',');
			Sb.Append(cs.opened); // Cumulative. Not reset on each interval.
			Sb.Append(',');
			Sb.Append(cs.closed); // Cumulative. Not reset on each interval.
		}

		private void WriteLine()
		{
			try
			{
				Sb.Append(System.Environment.NewLine);
				Writer.Write(Sb.ToString());
				Size += Sb.Length;
				Writer.Flush();

				if (MaxSize > 0 && Size >= MaxSize)
				{
					Writer.Close();

					// This call is recursive since Open() calls WriteLine() to write the header.
					Open();
				}
			}
			catch (IOException ioe)
			{
				Enabled = false;

				try
				{
					Writer.Close();
				}
				catch (Exception)
				{
				}

				throw new AerospikeException(ioe);
			}
		}

		private void GetCpuMemoryUsage(out double cpu, out long memory)
		{
			Process currentProcess = System.Diagnostics.Process.GetCurrentProcess();
			memory = currentProcess.WorkingSet64 + currentProcess.VirtualMemorySize64 + currentProcess.PagedMemorySize64;

			var currentTime = DateTime.UtcNow;
			var currentCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;

			var cpuUsedMs = (currentCpuUsage - prevCpuUsage).TotalMilliseconds;
			var totalMsPassed = (currentTime - prevTime).TotalMilliseconds;

			cpu = cpuUsedMs / (Environment.ProcessorCount * totalMsPassed) * 100;

			prevTime = currentTime;
			prevCpuUsage = currentCpuUsage;
		}
	}
}
