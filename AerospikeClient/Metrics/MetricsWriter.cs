/* 
 * Copyright 2012-2026 Aerospike, Inc.
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

using System.Diagnostics;
using System.Text;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	/// <summary>
	/// Client metrics listener.
	/// </summary>
	public sealed class MetricsWriter : IMetricsListener, IMetricsExporter, IDisposable
	{
		private static readonly string filenameFormat = "yyyyMMddHHmmss";
		private static readonly string timestampFormat = "yyyy-MM-dd HH:mm:ss";
		private static readonly int minFileSize = 1000000;

		private readonly object sync = new();
		private readonly string dir;
		private readonly StringBuilder sb;
		private StreamWriter writer;
		private long size;
		private long maxSize;
		private MetricsLatencyUnit latencyUnit;
		private int latencyColumns;
		private int latencyShift;
		private bool enabled;
		private bool disposed;
		private DateTime prevTime;
		private TimeSpan prevCpuUsage;

		/// <summary>
		/// Initialize metrics writer.
		/// </summary>
		public MetricsWriter(string dir)
			: this(dir, 0, 7, 1)
		{
		}

		/// <summary>
		/// Initialize an application-owned metrics file exporter.
		/// </summary>
		/// <param name="dir">Directory in which metrics files are created.</param>
		/// <param name="reportSizeLimit">Soft file size limit, or zero for unlimited.</param>
		/// <param name="latencyColumns">Latency histogram column count.</param>
		/// <param name="latencyShift">Latency histogram power-of-two shift.</param>
		public MetricsWriter(
			string dir,
			long reportSizeLimit,
			int latencyColumns = 7,
			int latencyShift = 1)
		{
			this.dir = dir;
			this.sb = new StringBuilder(8192);
			this.maxSize = reportSizeLimit;
			this.latencyUnit = MetricsLatencyUnit.Milliseconds;
			this.latencyColumns = latencyColumns;
			this.latencyShift = latencyShift;
			this.prevTime = DateTime.UtcNow;
			this.prevCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;
			ValidateSize(reportSizeLimit);
		}

		/// <summary>
		/// Open timestamped metrics file in Append mode and write header indicating what metrics will
		/// be stored.
		/// </summary>

		public void OnEnable(Cluster cluster, MetricsPolicy policy)
		{
			lock (sync)
			{
				ThrowIfDisposed();
				ValidateSize(policy.ReportSizeLimit);

				this.maxSize = policy.ReportSizeLimit;
				this.latencyUnit = policy.LatencyUnit;
				this.latencyColumns = policy.LatencyColumns;
				this.latencyShift = policy.LatencyShift;

				try
				{
					Directory.CreateDirectory(dir);
					Open();
				}
				catch (Exception e)
				{
					enabled = false;

					try
					{
						writer?.Dispose();
					}
					catch
					{
					}
					writer = null;

					if (e is AerospikeException)
					{
						throw;
					}
					throw new AerospikeException(e);
				}

				enabled = true;
			}
		}

		/// <summary>
		/// Write cluster metrics snapshot to file.
		/// </summary>
		public void OnSnapshot(Cluster cluster)
		{
			lock (sync)
			{
				ThrowIfDisposed();
				if (enabled)
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
			lock (sync)
			{
				ThrowIfDisposed();
				if (enabled)
				{
					sb.Append(DateTime.Now.ToString(timestampFormat));
					sb.Append(" node");
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
			lock (sync)
			{
				if (writer != null)
				{
					try
					{
						enabled = false;
						if (writer.BaseStream.CanWrite)
						{
							WriteCluster(cluster);
						}
					}
					catch (Exception e)
					{
						Log.Error("Failed to close metrics writer: " + Util.GetErrorMessage(e));
					}
					finally
					{
						CloseWriter();
					}
				}
			}
		}

		/// <summary>
		/// Export an immutable snapshot to the learn-metrics file format.
		/// The file is opened lazily on the first export.
		/// </summary>
		public void Export(MetricsSnapshot snapshot)
		{
			if (snapshot == null)
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Metrics snapshot is required.");
			}
			if (snapshot.LegacyFile == null)
			{
				throw new AerospikeException(
					ResultCode.PARAMETER_ERROR,
					"Metrics snapshot does not contain the legacy file payload.");
			}

			lock (sync)
			{
				ThrowIfDisposed();

				try
				{
					if (writer != null &&
						(latencyUnit != snapshot.LatencyUnit ||
							latencyColumns != snapshot.LatencyColumns ||
							latencyShift != snapshot.LatencyShift))
					{
						CloseWriter();
					}

					if (writer == null)
					{
						Directory.CreateDirectory(dir);
						latencyUnit = snapshot.LatencyUnit;
						latencyColumns = snapshot.LatencyColumns;
						latencyShift = snapshot.LatencyShift;
						Open();
						enabled = true;
					}
					WriteCluster(snapshot);
				}
				catch
				{
					enabled = false;
					sb.Clear();
					CloseWriter();
					throw;
				}
			}
		}

		/// <summary>
		/// Flush and close this application-owned writer.
		/// </summary>
		public void Dispose()
		{
			lock (sync)
			{
				if (disposed)
				{
					return;
				}
				disposed = true;
				enabled = false;
				CloseWriter();
			}
		}

		private void Open()
		{
			DateTime now = DateTime.Now;
			string stem = dir + Path.DirectorySeparatorChar + "metrics-" +
				now.ToString(filenameFormat);
			int collision = 0;

			while (true)
			{
				string path = collision == 0
					? stem + ".log"
					: stem + "-" + collision + ".log";

				try
				{
					writer = new StreamWriter(
						new FileStream(
							path,
							FileMode.CreateNew,
							FileAccess.Write,
							FileShare.Read));
					break;
				}
				catch (IOException) when (File.Exists(path))
				{
					collision++;
				}
			}
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
			sb.Append(cluster.client.clientVersion);
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

		private void WriteCluster(MetricsSnapshot snapshot)
		{
			LegacyMetricsFileSnapshot legacy = snapshot.LegacyFile;

			sb.Append(DateTime.Now.ToString(timestampFormat));
			sb.Append(" cluster[");
			sb.Append(legacy.ClusterName);
			sb.Append(",c#,");
			sb.Append(legacy.ClientVersion);
			sb.Append(',');
			sb.Append(legacy.AppId);
			sb.Append(',');
			WriteLabels(legacy.Labels);
			sb.Append(',');
			sb.Append((int)legacy.CpuPercent);
			sb.Append(',');
			sb.Append(legacy.MemoryBytes);
			sb.Append(',');
			sb.Append(legacy.RecoverQueueSize);
			sb.Append(',');
			sb.Append(legacy.InvalidNodeCount);
			sb.Append(',');
			sb.Append(legacy.CommandCount);
			sb.Append(',');
			sb.Append(legacy.RetryCount);
			sb.Append(',');
			sb.Append(legacy.DelayQueueTimeoutCount);
			sb.Append(',');
			sb.Append(legacy.AsyncThreadsInUse);
			sb.Append(',');
			sb.Append(legacy.AsyncCompletionPortsInUse);
			sb.Append(",[");

			for (int i = 0; i < legacy.Nodes.Count; i++)
			{
				if (i > 0)
				{
					sb.Append(',');
				}
				WriteNode(legacy.Nodes[i]);
			}
			sb.Append(']');
			WriteLine();

			foreach (LegacyMetricsFileNodeSnapshot departed in legacy.NodesDeparted)
			{
				sb.Append(DateTime.Now.ToString(timestampFormat));
				sb.Append(" node");
				WriteNode(departed);
				WriteLine();
			}
		}

		private void WriteLabels(IReadOnlyDictionary<string, string> labels)
		{
			if (labels == null || labels.Count == 0)
			{
				return;
			}

			sb.Append('[');
			int index = 0;

			foreach (KeyValuePair<string, string> label in labels)
			{
				if (index++ > 0)
				{
					sb.Append(',');
				}
				sb.Append('[').Append(label.Key).Append(',').Append(label.Value).Append(']');
			}
			sb.Append(']');
		}

		private void WriteNode(LegacyMetricsFileNodeSnapshot node)
		{
			sb.Append('[');
			sb.Append(node.Name);
			sb.Append(',');
			sb.Append(node.Address);
			sb.Append(',');
			sb.Append(node.Port);
			sb.Append(',');
			WriteConn(node.SyncConnections);
			sb.Append(',');
			WriteConn(node.AsyncConnections);
			sb.Append(",[");

			if (node.Namespaces.Count > 0)
			{
				foreach (LegacyMetricsFileNamespaceSnapshot ns in node.Namespaces)
				{
					WriteNamespace(ns);
				}
				sb.Remove(sb.Length - 2, 2);
			}
			sb.Append("]]");
			sb.Append("]]");
		}

		private void WriteNamespace(LegacyMetricsFileNamespaceSnapshot ns)
		{
			sb.Append(ns.Name);
			sb.Append(',');
			sb.Append(ns.Errors);
			sb.Append(',');
			sb.Append(ns.Timeouts);
			sb.Append(',');
			sb.Append(ns.KeyBusy);
			sb.Append(',');
			sb.Append(ns.BytesIn);
			sb.Append(',');
			sb.Append(ns.BytesOut);
			sb.Append(",[");

			for (int i = 0; i < ns.Latency.Count; i++)
			{
				if (i > 0)
				{
					sb.Append(',');
				}
				sb.Append(LatencyTypeToString((LatencyType)i));
				sb.Append('[');
				IReadOnlyList<long> buckets = ns.Latency[i];

				for (int j = 0; j < buckets.Count; j++)
				{
					if (j > 0)
					{
						sb.Append(',');
					}
					sb.Append(buckets[j]);
				}
				sb.Append(']');
			}
			sb.Append("]],[");
		}

		private void WriteNode(NodeMetricsSnapshot node)
		{
			sb.Append('[');
			sb.Append(node.Name);
			sb.Append(',');
			sb.Append(node.Address);
			sb.Append(',');
			sb.Append(node.Port);
			sb.Append(',');
			WriteConn(node.Connections);
			sb.Append(',');
			WriteConn(ConnectionMetricsSnapshot.Empty);
			sb.Append(",[");

			if (node.Namespaces.Count > 0)
			{
				foreach (NamespaceMetricsSnapshot ns in node.Namespaces)
				{
					sb.Append(ns.Name);
					sb.Append(',');
					sb.Append(ns.Errors);
					sb.Append(',');
					sb.Append(ns.Timeouts);
					sb.Append(',');
					sb.Append(ns.KeyBusy);
					sb.Append(',');
					sb.Append(ns.BytesIn);
					sb.Append(',');
					sb.Append(ns.BytesOut);
					sb.Append(",[");
					WriteLatency(LatencyType.CONN, ns.Latency.Connection, false);
					WriteLatency(LatencyType.READ, ns.Latency.Read, true);
					WriteLatency(LatencyType.WRITE, ns.Latency.Write, true);
					WriteLatency(LatencyType.BATCH, ns.Latency.Batch, true);
					WriteLatency(LatencyType.QUERY, ns.Latency.Query, true);
					sb.Append("]],[");
				}
				sb.Remove(sb.Length - 2, 2);
			}
			sb.Append("]]");
			sb.Append("]]");
		}

		private void WriteLatency(
			LatencyType type,
			IReadOnlyList<long> buckets,
			bool prependComma)
		{
			if (prependComma)
			{
				sb.Append(',');
			}
			sb.Append(LatencyTypeToString(type));
			sb.Append('[');

			for (int i = 0; i < buckets.Count; i++)
			{
				if (i > 0)
				{
					sb.Append(',');
				}
				sb.Append(buckets[i]);
			}
			sb.Append(']');
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

			using NodeMetricsLease metricsLease = node.AcquireMetrics();
			NodeMetrics nodeMetrics = metricsLease.Metrics;
			Histograms hGrams = nodeMetrics?.Histograms;
			ConcurrentHashMap<string, LatencyBuckets[]> hMap = hGrams?.histoMap;
			int max = Latency.GetMax();

			if (hMap != null)
			{
				foreach (string ns in hMap.Keys)
				{
					sb.Append(ns).Append(',');
					sb.Append(node.GetErrorCountByNS(ns));
					sb.Append(',');
					sb.Append(node.GetTimeoutCountbyNS(ns));
					sb.Append(',');
					sb.Append(node.GetKeyBusyCountByNS(ns));
					sb.Append(',');
					sb.Append(nodeMetrics.BytesInCounter?.GetCountByNS(ns) ?? 0);
					sb.Append(',');
					sb.Append(nodeMetrics.BytesOutCounter?.GetCountByNS(ns) ?? 0);
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
			}
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

		private void WriteConn(ConnectionMetricsSnapshot cs)
		{
			sb.Append(cs.InUse);
			sb.Append(',');
			sb.Append(cs.InPool);
			sb.Append(',');
			sb.Append(cs.Opened);
			sb.Append(',');
			sb.Append(cs.Closed);
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

		private static void ValidateSize(long reportSizeLimit)
		{
			if (reportSizeLimit != 0 && reportSizeLimit < minFileSize)
			{
				throw new AerospikeException(
					"MetricsPolicy.reportSizeLimit " + reportSizeLimit +
					" must be at least " + minFileSize);
			}
		}

		private void CloseWriter()
		{
			try
			{
				writer?.Dispose();
			}
			catch (Exception e)
			{
				Log.Error("Failed to dispose metrics writer: " +
					Util.GetErrorMessage(e));
			}
			writer = null;
		}

		private void ThrowIfDisposed()
		{
			if (disposed)
			{
				throw new ObjectDisposedException(nameof(MetricsWriter));
			}
		}
	}
}
