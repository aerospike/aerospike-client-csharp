/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
using System.Text;
using System.Diagnostics;

namespace Aerospike.Client
{
	/// <summary>
	/// Client metrics listener.
	/// </summary>
	public sealed class MetricsWriter : IMetricsListener
	{
		private static readonly string filenameFormat = "yyyyMMddHHmmss";
		private static readonly string timestampFormat = "yyyy-MM-dd HH:mm:ss";
		private static readonly int minFileSize = 1000000;

		private readonly string dir;
		private readonly StringBuilder sb;
		private StreamWriter writer;
		private long size;
		private long maxSize;
		private int latencyColumns;
		private int latencyShift;
		private bool enabled;
		private DateTime prevTime;
		private TimeSpan prevCpuUsage;

		/// <summary>
		/// Initialize metrics writer.
		/// </summary>
		public MetricsWriter(string dir)
		{
			this.dir = dir;
			this.sb = new StringBuilder(8192);
			this.prevTime = DateTime.UtcNow;
			this.prevCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;
		}

		/// <summary>
		/// Open timestamped metrics file in Append mode and write header indicating what metrics will
		/// be stored.
		/// </summary>

		public void OnEnable(Cluster cluster, MetricsPolicy policy)
		{
			if (policy.ReportSizeLimit != 0 && policy.ReportSizeLimit < minFileSize)
			{
				throw new AerospikeException("MetricsPolicy.reportSizeLimit " + policy.ReportSizeLimit +
					" must be at least " + minFileSize);
			}

			this.maxSize = policy.ReportSizeLimit;
			this.latencyColumns = policy.LatencyColumns;
			this.latencyShift = policy.LatencyShift;

			try
			{
				Directory.CreateDirectory(dir);
				Open();
			}
			catch (IOException ioe)
			{
				throw new AerospikeException(ioe);
			}

			enabled = true;
		}

		/// <summary>
		/// Write cluster metrics snapshot to file.
		/// </summary>
		public void OnSnapshot(Cluster cluster)
		{
			if (enabled)
			{
				WriteCluster(cluster);
			}
		}

		/// <summary>
		/// Write final node metrics snapshot on node that will be closed.
		/// </summary>
		public void OnNodeClose(Node node)
		{
			if (enabled)
			{
				sb.Append(DateTime.Now.ToString(timestampFormat));
				sb.Append(" node");
				WriteNode(node);
				WriteLine();
			}
		}

		/// <summary>
		/// Write final cluster metrics snapshot to file and then close the file.
		/// </summary>
		public void OnDisable(Cluster cluster)
		{
			if (enabled)
			{
				try
				{
					enabled = false;
					WriteCluster(cluster);
					writer.Close();
				}
				catch (Exception e)
				{
					Log.Error("Failed to close metrics writer: " + Util.GetErrorMessage(e));
				}
			}
		}

		private void Open()
		{
			DateTime now = DateTime.Now;
			string path = dir + Path.DirectorySeparatorChar + "metrics-" + now.ToString(filenameFormat) + ".log";
			writer = new StreamWriter(path, false);
			size = 0;

			sb.Append(now.ToString(timestampFormat));
			sb.Append(" header(2)");
			sb.Append(" cluster[name,clientType,clientVersion,appId,label[],cpu,mem,recoverQueueSize,invalidNodeCount,commandCount,retryCount,delayQueueTimeoutCount,asyncThreadsInUse,asyncCompletionPortsInUse,node[]]");
			sb.Append(" label[name,value]");
			sb.Append(" node[name,address,port,syncConn,asyncConn,namespace[]]");
			sb.Append(" conn[inUse,inPool,opened,closed]");
			sb.Append(" namespace[name,errors,timeouts,keyBusy,bytesIn,bytesOut,latency[]]");
			sb.Append(" latency(");
			sb.Append(latencyColumns);
			sb.Append(',');
			sb.Append(latencyShift);
			sb.Append(')');
			sb.Append("[type[l1,l2,l3...]]");
			WriteLine();
		}

		private void WriteCluster(Cluster cluster)
		{
			MetricsPolicy policy = cluster.MetricsPolicy;
			string clusterName = cluster.clusterName;
			clusterName ??= "";

			GetCpuMemoryUsage(out double cpu, out long mem);

			sb.Append(DateTime.Now.ToString(timestampFormat));
			sb.Append(" cluster[");
			sb.Append(clusterName);
			sb.Append(',');
			sb.Append("c#");
			sb.Append(',');
			sb.Append(cluster.client.version);
			sb.Append(',');
			if (cluster.appId != null)
			{
				sb.Append(cluster.appId);
			}
			else
			{
				byte[] userBytes = cluster.user;
				if (userBytes != null && userBytes.Length > 0)
				{
					string user = ByteUtil.Utf8ToString(userBytes, 0, userBytes.Length);
					sb.Append(user);
				}
			}
			sb.Append(',');
			if (policy.labels != null)
			{
				sb.Append('[');
				foreach (string key in policy.labels.Keys)
				{
					sb.Append('[').Append(key).Append(',').Append(policy.labels[key]).Append("],");
				}
				sb.Remove(sb.Length - 1, 1); // Remove last comma
				sb.Append(']');
			}
			sb.Append(',');
			sb.Append((int)cpu);
			sb.Append(',');
			sb.Append(mem);
			sb.Append(',');
			sb.Append(cluster.GetRecoverQueueSize());
			sb.Append(',');
			sb.Append(cluster.InvalidNodeCount); // Cumulative. Not reset on each interval.
			sb.Append(',');
			sb.Append(cluster.GetCommandCount());  // Cumulative. Not reset on each interval.
			sb.Append(',');
			sb.Append(cluster.GetRetryCount()); // Cumulative. Not reset on each interval.
			sb.Append(',');
			sb.Append(cluster.GetDelayQueueTimeoutCount()); // Cumulative. Not reset on each interval.
			sb.Append(',');

			ThreadPool.GetMaxThreads(out int workerThreadsMax, out int completionPortThreadsMax);
			ThreadPool.GetAvailableThreads(out int workerThreads, out int completionPortThreads);

			var threadsInUse = workerThreadsMax - workerThreads;
			var completionPortsInUse = completionPortThreadsMax - completionPortThreads;

			sb.Append(threadsInUse);
			sb.Append(',');
			sb.Append(completionPortsInUse);
			sb.Append(",[");

			Node[] nodes = cluster.Nodes;

			for (int i = 0; i < nodes.Length; i++)
			{
				Node node = nodes[i];

				if (i > 0)
				{
					sb.Append(',');
				}
				WriteNode(node);
			}
			sb.Append(']');
			WriteLine();
		}

		private void WriteNode(Node node)
		{
			sb.Append('[');
			sb.Append(node.Name);
			sb.Append(',');

			Host host = node.Host;

			sb.Append(host.name);
			sb.Append(',');
			sb.Append(host.port);
			sb.Append(',');

			WriteConn(node.GetConnectionStats());
			sb.Append(',');
			var asyncStats = new ConnectionStats(0, 0, 0, 0);
			if (node is AsyncNode async)
			{
				asyncStats = async.GetAsyncConnectionStats();
			}
			WriteConn(asyncStats);
			sb.Append(",[");

			Histograms hGrams = node.GetMetrics().Histograms;
			ConcurrentHashMap<string, LatencyBuckets[]> hMap = hGrams.histoMap;
			int max = Latency.GetMax();

			foreach (string ns in hMap.Keys)
			{
				sb.Append(ns).Append(',');
				sb.Append(node.GetErrorCountByNS(ns));
				sb.Append(',');
				sb.Append(node.GetTimeoutCountbyNS(ns));
				sb.Append(',');
				sb.Append(node.GetKeyBusyCountByNS(ns));
				sb.Append(',');
				sb.Append(node.GetBytesInByNS(ns));
				sb.Append(',');
				sb.Append(node.GetBytesOutByNS(ns));
				sb.Append(",[");
				LatencyBuckets[] latencyBuckets = hGrams.GetBuckets(ns);
				for (int i = 0; i < max; i++)
				{
					if (i > 0)
					{
						sb.Append(',');
					}

					sb.Append(LatencyTypeToString((LatencyType)i));
					sb.Append('[');

					LatencyBuckets buckets = latencyBuckets[i];
					int bucketMax = buckets.GetMax();
					for (int j = 0; j < bucketMax; j++)
					{
						if (j > 0)
						{
							sb.Append(',');
						}
						sb.Append(buckets.GetBucket(j)); // Cumulative. Not reset on each interval.
					}
					sb.Append(']');
				}
				sb.Append("]],[");
			}
			sb.Remove(sb.Length - 2, 2); // Remove ,[
			sb.Append("]]");
			sb.Append("]]");
		}

		private void WriteConn(ConnectionStats cs)
		{
			sb.Append(cs.inUse);
			sb.Append(',');
			sb.Append(cs.inPool);
			sb.Append(',');
			sb.Append(cs.opened); // Cumulative. Not reset on each interval.
			sb.Append(',');
			sb.Append(cs.closed); // Cumulative. Not reset on each interval.
		}

		private void WriteLine()
		{
			try
			{
				sb.Append(System.Environment.NewLine);
				writer.Write(sb.ToString());
				writer.Flush();
				size += sb.Length;
				sb.Clear();

				if (maxSize > 0 && size >= maxSize)
				{
					writer.Close();

					// This call is recursive since Open() calls WriteLine() to write the header.
					Open();
				}
			}
			catch (IOException ioe)
			{
				enabled = false;

				try
				{
					writer.Close();
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
