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
using System.IO;
using System.Text;
using System.Diagnostics;

namespace Aerospike.Client
{
	public sealed class MetricsWriter
    {
		internal readonly LatencyManager connLatency;
		internal readonly LatencyManager writeLatency;
		internal readonly LatencyManager readLatency;
		internal readonly LatencyManager batchLatency;
		private readonly StringBuilder sb;
		private readonly StreamWriter writer;
		private DateTime beginTime;
		private TimeSpan beginSpan;

		public MetricsWriter(MetricsPolicy policy)
        {
			connLatency = new LatencyManager(policy, "conn");
			writeLatency = new LatencyManager(policy, "write");
			readLatency = new LatencyManager(policy, "read");
			batchLatency = new LatencyManager(policy, "batch");
			sb = new StringBuilder(256);
			
			FileStream fs = new FileStream(policy.reportPath, FileMode.Append, FileAccess.Write);
			writer = new StreamWriter(fs);
			writer.WriteLine(writeLatency.PrintHeader(sb));

			beginTime = DateTime.UtcNow;
			beginSpan = Process.GetCurrentProcess().TotalProcessorTime;
		}
	
		public void Write(Cluster cluster)
		{
			ClusterStats stats = cluster.GetStats();
			int threadExpandCount = cluster.ResetThreadExpandCount();

			Process proc = Process.GetCurrentProcess();
			TimeSpan endSpan = proc.TotalProcessorTime;
			DateTime endTime = DateTime.UtcNow;
			double cpu = ((endSpan - beginSpan).TotalMilliseconds * 100.0) / (Environment.ProcessorCount * (endTime - beginTime).TotalMilliseconds);
			long mem = proc.PrivateMemorySize64;

			lock (writer)
			{		
				sb.Length = 0;
				sb.Append("entry ");
				sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
				sb.Append(' ');
				sb.Append(cpu);
				sb.Append(' ');
				sb.Append(mem);
				sb.Append(' ');
				sb.Append(threadExpandCount);
				sb.Append(' ');
				sb.Append(stats.threadsInUse);
				sb.Append(' ');
				sb.Append(stats.completionPortsInUse);
				writer.WriteLine(sb.ToString());

				WriteLine(connLatency);
				WriteLine(writeLatency);
				WriteLine(readLatency);
				WriteLine(batchLatency);

				foreach (NodeStats ns in stats.nodes)
				{
					sb.Length = 0;
					sb.Append("node ");
					sb.Append(ns.node);

					ConnectionStats cs = ns.syncStats;
					sb.Append(' ');
					sb.Append(cs.inUse);
					sb.Append(' ');
					sb.Append(cs.inPool);
					sb.Append(' ');
					sb.Append(cs.opened);
					sb.Append(' ');
					sb.Append(cs.closed);
					sb.Append(' ');

					int timeoutCount = ns.node.ResetTimeoutCount();
					sb.Append(timeoutCount);

					writer.WriteLine(sb.ToString());
				}
			}
		}

		private void WriteLine(LatencyManager lm)
		{
			string line = lm.PrintResults(sb);

			if (line != null)
			{
				writer.WriteLine(line);
			}
		}

		public void Close(Cluster cluster)
		{
			Write(cluster);

			lock (writer)
			{
				writer.Close();
			}
		}
	}
}
