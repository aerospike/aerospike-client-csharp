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
using System.Text.RegularExpressions;

namespace Aerospike.Test
{
	/// <summary>
	/// Kind cluster running the Aerospike Kubernetes Operator, built by
	/// <c>.github/workflows/discovery-k8s.yaml</c> or <c>AerospikeTest/discovery/run_l3.sh</c>.
	/// The cluster arrives as environment variables:
	/// <list type="bullet">
	/// <item><c>DISCOVERY_KUBECONFIG</c>: kubeconfig of the kind cluster; without it every test is skipped</item>
	/// <item><c>DISCOVERY_ENDPOINT_HOSTNAME</c>: hostname the client and the server both resolve</item>
	/// <item><c>DISCOVERY_INGRESS_PORTS</c>: ingress-nginx tcp-services host ports</item>
	/// <item><c>DISCOVERY_SERVICE_NODE_PORTS</c>: NodePorts of the per-pod Services at setup, in pod order</item>
	/// <item><c>DISCOVERY_TLS_CA_FILE</c>: CA file of the server certificate, set once
	/// <c>.github/discovery/enable-l3-tls.sh</c> serves TLS on the AerospikeCluster</item>
	/// <item><c>DISCOVERY_TLS_NAME</c>: tls-name that script configures</item>
	/// </list>
	/// The AerospikeCluster, its pods, their Services and the Kubernetes nodes are read from the API
	/// server at runtime through kubectl, every call bounded by <see cref="KubectlTimeoutMs"/>.
	/// </summary>
	internal sealed class K8sCluster
	{
		public const string AlternateAccessLabel = "aerospike.com/configured-alternate-access-address";
		public const string Skip = "needs a kind cluster running the Aerospike Kubernetes Operator: run " +
			"AerospikeTest/discovery/run_l3.sh or the discovery-k8s workflow (DISCOVERY_KUBECONFIG is not set)";
		public const long KubectlTimeoutMs = 120000;
		public const long ScaleTimeoutMs = 900000;

		public static readonly string Kubeconfig = Optional("DISCOVERY_KUBECONFIG");
		public static readonly string EndpointHostname = Optional("DISCOVERY_ENDPOINT_HOSTNAME");
		public static readonly IReadOnlyList<int> IngressPorts = Ports(Optional("DISCOVERY_INGRESS_PORTS"));
		public static readonly IReadOnlyList<int> ServiceNodePorts = Ports(Optional("DISCOVERY_SERVICE_NODE_PORTS"));
		public static readonly string TlsCaFile = Optional("DISCOVERY_TLS_CA_FILE");
		public static readonly string TlsName = Optional("DISCOVERY_TLS_NAME");

		/// <summary>
		/// Aerospike pod as the API server reports it.
		/// </summary>
		public sealed class Pod
		{
			public string Name { get; }
			public string Node { get; }
			public string Ip { get; }
			public string Phase { get; }

			public Pod(string name, string node, string ip, string phase)
			{
				Name = name;
				Node = node;
				Ip = ip;
				Phase = phase;
			}

			public override string ToString()
			{
				return Name + " node=" + Node + " ip=" + Ip + " phase=" + Phase;
			}
		}

		private readonly string clusterNamespace;
		private readonly string clusterName;

		private K8sCluster(string clusterNamespace, string clusterName)
		{
			this.clusterNamespace = clusterNamespace;
			this.clusterName = clusterName;
		}

		/// <summary>
		/// Return the one AerospikeCluster in the kind cluster.
		/// </summary>
		public static K8sCluster Find()
		{
			List<string> lines = Lines(Kubectl("get", "aerospikecluster", "--all-namespaces", "-o",
				"jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{\"\\n\"}{end}"));
			Assert.AreEqual(1, lines.Count, "AerospikeClusters " + Format(lines));
			string[] parts = lines[0].Split(' ');
			return new K8sCluster(parts[0], parts[1]);
		}

		public string Namespace => clusterNamespace;

		public string Name => clusterName;

		public string Get(string jsonpath)
		{
			return Kubectl("-n", clusterNamespace, "get", "aerospikecluster", clusterName, "-o", "jsonpath={" + jsonpath + "}").Trim();
		}

		public int Size()
		{
			return int.Parse(Get(".spec.size"));
		}

		public bool MultiPodPerHost()
		{
			return bool.Parse(Get(".spec.podSpec.multiPodPerHost"));
		}

		public int ServicePort()
		{
			return int.Parse(Get(".spec.aerospikeConfig.network.service.port"));
		}

		public List<Pod> Pods()
		{
			List<Pod> pods = new List<Pod>();

			foreach (string line in Lines(Kubectl("-n", clusterNamespace, "get", "pods", "-l", "aerospike.com/cr=" + clusterName, "-o",
				"jsonpath={range .items[*]}{.metadata.name} {.spec.nodeName} {.status.podIP} {.status.phase}{\"\\n\"}{end}")))
			{
				string[] parts = line.Split(' ', StringSplitOptions.None);
				pods.Add(new Pod(parts[0], parts[1], parts[2], parts[3]));
			}
			pods.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));
			return pods;
		}

		/// <summary>
		/// Return the NodePort of the pod's own Service for <paramref name="port"/>, or 0 when it has none.
		/// </summary>
		public int NodePort(string pod, int port)
		{
			string value = Kubectl("-n", clusterNamespace, "get", "service", pod, "--ignore-not-found", "-o",
				"jsonpath={.spec.ports[?(@.port==" + port + ")].nodePort}").Trim();
			return value.Length == 0 ? 0 : int.Parse(value);
		}

		/// <summary>
		/// Return the hostPort the pod's containers publish <paramref name="port"/> on, or 0 when none does.
		/// </summary>
		public int HostPort(string pod, int port)
		{
			string value = Kubectl("-n", clusterNamespace, "get", "pod", pod, "-o",
				"jsonpath={.spec.containers[*].ports[?(@.containerPort==" + port + ")].hostPort}").Trim();
			return value.Length == 0 ? 0 : int.Parse(value.Split(' ')[0]);
		}

		/// <summary>
		/// Return the UID of the object, or an empty string when it does not exist.
		/// </summary>
		public string Uid(string kind, string objectName)
		{
			return Kubectl("-n", clusterNamespace, "get", kind, objectName, "--ignore-not-found", "-o",
				"jsonpath={.metadata.uid}").Trim();
		}

		public string PodReadyAt(string pod)
		{
			return Kubectl("-n", clusterNamespace, "get", "pod", pod, "-o",
				"jsonpath={.status.conditions[?(@.type==\"Ready\")].lastTransitionTime}").Trim();
		}

		public string ReadinessProbe(string pod)
		{
			return Kubectl("-n", clusterNamespace, "get", "pod", pod, "-o",
				"jsonpath={.spec.containers[?(@.name==\"aerospike-server\")].readinessProbe}").Trim();
		}

		public string PublishNotReadyAddresses(string service)
		{
			return Kubectl("-n", clusterNamespace, "get", "service", service, "-o",
				"jsonpath={.spec.publishNotReadyAddresses}").Trim();
		}

		/// <summary>
		/// Return the number of ready endpoints in the Service's EndpointSlices.
		/// </summary>
		public int ReadyEndpoints(string service)
		{
			string ready = Kubectl("-n", clusterNamespace, "get", "endpointslices", "-l",
				"kubernetes.io/service-name=" + service, "-o",
				"jsonpath={range .items[*].endpoints[*]}{.conditions.ready} {end}");
			int count = 0;

			foreach (string value in ready.Trim().Split(' '))
			{
				if (value == "true")
				{
					count++;
				}
			}
			return count;
		}

		public void DeletePod(string pod)
		{
			Kubectl("-n", clusterNamespace, "delete", "pod", pod, "--wait=true");
		}

		/// <summary>
		/// Create a NodePort Service that sends <paramref name="port"/> on <paramref name="nodePort"/> to
		/// <paramref name="pod"/> alone.
		/// </summary>
		public void CreatePodNodePortService(string service, string pod, int port, int nodePort)
		{
			Kubectl("-n", clusterNamespace, "create", "service", "nodeport", service, "--tcp=" + port + ':' + port,
				"--node-port=" + nodePort);
			Kubectl("-n", clusterNamespace, "set", "selector", "service", service,
				"statefulset.kubernetes.io/pod-name=" + pod);
		}

		public void DeleteService(string service)
		{
			Kubectl("-n", clusterNamespace, "delete", "service", service, "--ignore-not-found", "--wait=true");
		}

		/// <summary>
		/// Return the first and last port of the API server's NodePort range, read from the error the
		/// API server returns when asked to create a Service whose NodePort is outside it.
		/// </summary>
		public int[] NodePortRange()
		{
			string probe = clusterName + "-node-port-range";
			Assert.IsTrue(string.IsNullOrEmpty(Uid("service", probe)), "Service " + probe + " exists");

			try
			{
				string error = KubectlError("-n", clusterNamespace, "create", "service", "nodeport", probe, "--tcp=1:1",
					"--node-port=1");
				Match match = Regex.Match(error, "range of valid ports is (\\d+)-(\\d+)");
				Assert.IsTrue(match.Success, "No NodePort range in: " + error);
				return new int[] { int.Parse(match.Groups[1].Value), int.Parse(match.Groups[2].Value) };
			}
			finally
			{
				DeleteService(probe);
			}
		}

		/// <summary>
		/// Return the NodePorts every Service in the kind cluster holds.
		/// </summary>
		public static IReadOnlyList<int> AllocatedNodePorts()
		{
			return Ports(Kubectl("get", "service", "--all-namespaces", "-o",
				"jsonpath={range .items[*].spec.ports[*]}{.nodePort},{end}"));
		}

		/// <summary>
		/// Return the StatefulSet ordinal at the end of the pod's name.
		/// </summary>
		public static int Ordinal(string pod)
		{
			return int.Parse(pod.Substring(pod.LastIndexOf('-') + 1));
		}

		/// <summary>
		/// Return the decoded value of <paramref name="key"/> in the secret <paramref name="secret"/>.
		/// </summary>
		public byte[] SecretData(string secret, string key)
		{
			string escaped = key.Replace(".", "\\.");
			return Convert.FromBase64String(Kubectl("-n", clusterNamespace, "get", "secret", secret, "-o",
				"jsonpath={.data." + escaped + "}").Trim());
		}

		/// <summary>
		/// Set the AerospikeCluster size without waiting for the operator.
		/// </summary>
		public void Resize(int size)
		{
			Kubectl("-n", clusterNamespace, "patch", "aerospikecluster", clusterName, "--type", "merge",
				"-p", "{\"spec\":{\"size\":" + size + "}}");
		}

		public static string NodeLabel(string node)
		{
			return Kubectl("get", "node", node, "-o",
				"jsonpath={.metadata.labels." + AlternateAccessLabel.Replace(".", "\\.") + "}").Trim();
		}

		public static string NodeAddress(string node)
		{
			return Kubectl("get", "node", node, "-o",
				"jsonpath={.status.addresses[?(@.type==\"InternalIP\")].address}").Trim();
		}

		/// <summary>
		/// Set the AerospikeCluster size and wait until the operator reports it Completed at that size
		/// with that many running pods.
		/// </summary>
		public void Scale(int size)
		{
			Kubectl("-n", clusterNamespace, "patch", "aerospikecluster", clusterName, "--type", "merge",
				"-p", "{\"spec\":{\"size\":" + size + "}}");

			string[] state = new string[1];
			bool done = WaitFor(() =>
			{
				state[0] = Get(".status.size") + ' ' + Get(".status.phase") + ' ' + FormatPods(Pods());
				return IsScaled(size);
			}, ScaleTimeoutMs, 2000);
			Console.WriteLine("L3-SCALED cluster=" + clusterNamespace + '/' + clusterName + " size=" + size + " state=" + state[0]);
			Assert.IsTrue(done, "AerospikeCluster " + clusterName + " not Completed at size " + size + " within " +
				ScaleTimeoutMs + "ms: " + state[0]);
		}

		private bool IsScaled(int size)
		{
			if (!string.Equals(Get(".status.size"), size.ToString(), StringComparison.Ordinal) ||
				!string.Equals(Get(".status.phase"), "Completed", StringComparison.Ordinal))
			{
				return false;
			}

			List<Pod> pods = Pods();

			if (pods.Count != size)
			{
				return false;
			}

			foreach (Pod pod in pods)
			{
				if (!string.Equals(pod.Phase, "Running", StringComparison.Ordinal))
				{
					return false;
				}
			}
			return true;
		}

		public static bool WaitFor(Func<bool> condition, long timeoutMs, int pollMs)
		{
			long deadline = Environment.TickCount64 + timeoutMs;

			while (!condition())
			{
				if (Environment.TickCount64 >= deadline)
				{
					return false;
				}
				Thread.Sleep(pollMs);
			}
			return true;
		}

		/// <summary>
		/// Run kubectl against <see cref="Kubeconfig"/>, expecting it to fail, and return its standard
		/// error. Fails the test when kubectl exits zero or does not finish within
		/// <see cref="KubectlTimeoutMs"/>.
		/// </summary>
		public static string KubectlError(params string[] args)
		{
			List<string> command = new List<string> { "kubectl", "--request-timeout=30s" };
			command.AddRange(args);

			string stderrFile = Path.GetTempFileName();
			Process process = null;

			try
			{
				ProcessStartInfo start = new ProcessStartInfo
				{
					FileName = command[0],
					RedirectStandardInput = true,
					RedirectStandardOutput = true,
					RedirectStandardError = true,
					UseShellExecute = false,
					CreateNoWindow = true
				};
				for (int i = 1; i < command.Count; i++)
				{
					start.ArgumentList.Add(command[i]);
				}
				start.Environment["KUBECONFIG"] = Kubeconfig;

				process = Process.Start(start);
				Assert.IsNotNull(process, string.Join(' ', command) + " failed to start");
				process.StandardInput.Close();
				process.StandardOutput.ReadToEnd();

				if (!process.WaitForExit((int)KubectlTimeoutMs))
				{
					Assert.Fail(string.Join(' ', command) + " did not finish within " + KubectlTimeoutMs + "ms");
				}

				string stderr = process.StandardError.ReadToEnd();
				Assert.AreNotEqual(0, process.ExitCode, string.Join(' ', command) + " succeeded");
				return stderr.Trim();
			}
			catch (IOException e)
			{
				throw new InvalidOperationException(string.Join(' ', command) + " failed to run", e);
			}
			finally
			{
				if (process != null && !process.HasExited)
				{
					process.Kill(entireProcessTree: true);
				}
				File.Delete(stderrFile);
			}
		}

		/// <summary>
		/// Run kubectl against <see cref="Kubeconfig"/> and return its standard output. Fails the test when
		/// kubectl exits non-zero or does not finish within <see cref="KubectlTimeoutMs"/>.
		/// </summary>
		public static string Kubectl(params string[] args)
		{
			List<string> command = new List<string> { "kubectl", "--request-timeout=30s" };
			command.AddRange(args);

			string stdoutFile = Path.GetTempFileName();
			string stderrFile = Path.GetTempFileName();
			Process process = null;

			try
			{
				ProcessStartInfo start = new ProcessStartInfo
				{
					FileName = command[0],
					RedirectStandardInput = true,
					RedirectStandardOutput = true,
					RedirectStandardError = true,
					UseShellExecute = false,
					CreateNoWindow = true
				};
				for (int i = 1; i < command.Count; i++)
				{
					start.ArgumentList.Add(command[i]);
				}
				start.Environment["KUBECONFIG"] = Kubeconfig;

				process = Process.Start(start);
				Assert.IsNotNull(process, string.Join(' ', command) + " failed to start");
				process.StandardInput.Close();

				if (!process.WaitForExit((int)KubectlTimeoutMs))
				{
					Assert.Fail(string.Join(' ', command) + " did not finish within " + KubectlTimeoutMs + "ms");
				}

				File.WriteAllText(stdoutFile, process.StandardOutput.ReadToEnd());
				string stderr = process.StandardError.ReadToEnd();

				if (process.ExitCode != 0)
				{
					Assert.Fail(string.Join(' ', command) + " exited " + process.ExitCode + ": " + stderr.Trim());
				}
				return File.ReadAllText(stdoutFile, Encoding.UTF8);
			}
			catch (IOException e)
			{
				throw new InvalidOperationException(string.Join(' ', command) + " failed to run", e);
			}
			finally
			{
				if (process != null && !process.HasExited)
				{
					process.Kill(entireProcessTree: true);
				}
				File.Delete(stdoutFile);
				File.Delete(stderrFile);
			}
		}

		private static List<string> Lines(string output)
		{
			List<string> list = new List<string>();

			foreach (string line in output.Split('\n'))
			{
				if (!string.IsNullOrWhiteSpace(line))
				{
					list.Add(line.Trim());
				}
			}
			return list;
		}

		private static List<int> Ports(string csv)
		{
			List<int> list = new List<int>();

			foreach (string port in csv.Split(','))
			{
				if (port.Length > 0)
				{
					list.Add(int.Parse(port.Trim()));
				}
			}
			return list;
		}

		private static string Optional(string name)
		{
			string value = Environment.GetEnvironmentVariable(name);
			return value ?? "";
		}

		private static string Format(IEnumerable<string> values)
		{
			return "[" + string.Join(", ", values) + "]";
		}

		private static string FormatPods(List<Pod> pods)
		{
			List<string> parts = new List<string>();

			foreach (Pod pod in pods)
			{
				parts.Add(pod.ToString());
			}
			return Format(parts);
		}
	}
}
