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
		private Cluster listenerCluster;

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
				this.listenerCluster = cluster;

				try
				{
					Directory.CreateDirectory(dir);
					Open();
				}
				catch (Exception e)
				{
					enabled = false;
					listenerCluster = null;

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
					WriteCluster(cluster.GetMetricsSnapshot());
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
					MetricsSnapshot snapshot =
						listenerCluster?.GetMetricsSnapshot();
					NodeMetricsSnapshot nodeSnapshot = snapshot?.Nodes
						.FirstOrDefault(current => current.Name == node.Name);
					MetricsNodeCompatibilitySnapshot compatibility =
						snapshot?.Compatibility.Nodes.FirstOrDefault(
							current => current.Name == node.Name);
					nodeSnapshot ??= compatibility == null
						? null
						: new NodeMetricsSnapshot(
							compatibility.Name,
							compatibility.Address,
							compatibility.Port,
							0,
							ConnectionMetricsSnapshot.Empty,
							NodeLatencyMetricsSnapshot.Empty,
							Array.Empty<NamespaceMetricsSnapshot>());

					if (nodeSnapshot != null)
					{
						sb.Append(DateTime.Now.ToString(timestampFormat));
						sb.Append(" node");
						WriteNode(nodeSnapshot, compatibility);
						WriteLine();
					}
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
							WriteCluster(cluster.GetMetricsSnapshot());
						}
					}
					catch (Exception e)
					{
						Log.Error("Failed to close metrics writer: " + Util.GetErrorMessage(e));
					}
					finally
					{
						CloseWriter();
						listenerCluster = null;
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
				listenerCluster = null;
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

		private void WriteCluster(MetricsSnapshot snapshot)
		{
			MetricsCompatibilitySnapshot compatibility =
				snapshot.Compatibility;

			sb.Append(DateTime.Now.ToString(timestampFormat));
			sb.Append(" cluster[");
			sb.Append(snapshot.ClusterName);
			sb.Append(",c#,");
			sb.Append(snapshot.ClientVersion);
			sb.Append(',');
			sb.Append(compatibility.AppId);
			sb.Append(',');
			WriteLabels(snapshot.Labels);
			sb.Append(',');
			sb.Append((int)compatibility.CpuPercent);
			sb.Append(',');
			sb.Append(compatibility.MemoryBytes);
			sb.Append(',');
			sb.Append(compatibility.RecoverQueueSize);
			sb.Append(',');
			sb.Append(compatibility.InvalidNodeCount);
			sb.Append(',');
			sb.Append(compatibility.CommandCount);
			sb.Append(',');
			sb.Append(compatibility.RetryCount);
			sb.Append(',');
			sb.Append(compatibility.DelayQueueTimeoutCount);
			sb.Append(',');
			sb.Append(compatibility.AsyncThreadsInUse);
			sb.Append(',');
			sb.Append(compatibility.AsyncCompletionPortsInUse);
			sb.Append(",[");

			Dictionary<string, NodeMetricsSnapshot> activeNodes =
				snapshot.Nodes.ToDictionary(
					node => node.Name,
					StringComparer.Ordinal);

			for (int i = 0; i < compatibility.Nodes.Count; i++)
			{
				if (i > 0)
				{
					sb.Append(',');
				}
				MetricsNodeCompatibilitySnapshot nodeCompatibility =
					compatibility.Nodes[i];
				activeNodes.TryGetValue(
					nodeCompatibility.Name,
					out NodeMetricsSnapshot node);
				node ??= CompatibilityNode(nodeCompatibility);
				WriteNode(node, nodeCompatibility);
			}
			sb.Append(']');
			WriteLine();

			Dictionary<string, NodeMetricsSnapshot> departedNodes =
				snapshot.NodesDeparted.ToDictionary(
					node => node.Name,
					StringComparer.Ordinal);

			foreach (MetricsNodeCompatibilitySnapshot nodeCompatibility in
				compatibility.NodesDeparted)
			{
				sb.Append(DateTime.Now.ToString(timestampFormat));
				sb.Append(" node");
				departedNodes.TryGetValue(
					nodeCompatibility.Name,
					out NodeMetricsSnapshot departed);
				departed ??= CompatibilityNode(nodeCompatibility);
				WriteNode(departed, nodeCompatibility);
				WriteLine();
			}
		}

		private static NodeMetricsSnapshot CompatibilityNode(
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			return new NodeMetricsSnapshot(
				compatibility.Name,
				compatibility.Address,
				compatibility.Port,
				0,
				ConnectionMetricsSnapshot.Empty,
				NodeLatencyMetricsSnapshot.Empty,
				Array.Empty<NamespaceMetricsSnapshot>());
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

		private void WriteNode(
			NodeMetricsSnapshot node,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			sb.Append('[');
			sb.Append(node.Name);
			sb.Append(',');
			sb.Append(node.Address);
			sb.Append(',');
			sb.Append(node.Port);
			sb.Append(',');
			WriteConn(
				compatibility?.SyncConnections ??
					new ConnectionStats(0, 0, 0, 0));
			sb.Append(',');
			WriteConn(
				compatibility?.AsyncConnections ??
					new ConnectionStats(0, 0, 0, 0));
			sb.Append(",[");

			if (compatibility?.Namespaces.Count > 0)
			{
				foreach (MetricsNamespaceCompatibilitySnapshot ns in
					compatibility.Namespaces)
				{
					WriteNamespace(ns);
				}
				sb.Remove(sb.Length - 2, 2);
			}
			sb.Append("]]");
			sb.Append("]]");
		}

		private void WriteNamespace(MetricsNamespaceCompatibilitySnapshot ns)
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
