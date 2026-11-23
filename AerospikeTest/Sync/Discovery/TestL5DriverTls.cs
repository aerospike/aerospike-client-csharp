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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Aerospike.Client;
using Driver = Aerospike.L5Driver.L5Driver;

namespace Aerospike.Test
{
	/// <summary>
	/// The L5 driver's TLS flags against stub nodes listening with TLS on 127.0.0.1, whose
	/// certificate, signed by a CA made for the test, names only <see cref="TlsName"/>. Needs no
	/// server.
	/// </summary>
	[TestClass]
	public class TestL5DriverTls
	{
		private const string TlsName = "tls.l5.test";

		private static string dir;
		private static string caPem;

		[ClassInitialize]
		public static void Certificates(TestContext context)
		{
			dir = Path.Combine(Path.GetTempPath(), "l5-driver-tls-" + Guid.NewGuid().ToString("N"));
			Directory.CreateDirectory(dir);

			using (RSA caKey = RSA.Create(2048))
			{
				CertificateRequest caReq = new CertificateRequest(
					"CN=L5 Driver Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
				caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
				using (X509Certificate2 ca = caReq.CreateSelfSigned(
					DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2)))
				{
					caPem = Path.Combine(dir, "ca.pem");
					File.WriteAllText(caPem, PemEncode(ca.RawData, "CERTIFICATE"));
				}
			}
		}

		[ClassCleanup]
		public static void Cleanup()
		{
			if (dir != null && Directory.Exists(dir))
			{
				try
				{
					Directory.Delete(dir, true);
				}
				catch (IOException)
				{
				}
			}
		}

		[TestMethod]
		public void WithoutTlsNameConnectsInTheClear()
		{
			Driver.Options options = Options("pass-through", "127.0.0.1:3000");
			Assert.IsNull(options.Policy().tlsPolicy);
			Assert.IsNull(options.Seed.tlsName);
		}

		[TestMethod]
		public void TlsNameWithoutCaFileTrustsTheDefaultStore()
		{
			Driver.Options options = Options("pass-through", "127.0.0.1:3000", "--tls-name", TlsName);
			Assert.IsNotNull(options.Policy().tlsPolicy);
			Assert.AreEqual(new Host("127.0.0.1", TlsName, 3000), options.Seed);
			Assert.AreEqual(TlsName, options.Seed.tlsName);
		}

		[TestMethod]
		public void CaFileWithoutTlsNameIsABadFlag()
		{
			try
			{
				Options("pass-through", "127.0.0.1:3000", "--tls-ca-file", caPem);
				Assert.Fail("--tls-ca-file without --tls-name accepted");
			}
			catch (ArgumentException e)
			{
				Assert.AreEqual("--tls-ca-file only with --tls-name", e.Message);
			}

			Result result = DriverProcess("", "--seed", "127.0.0.1:3000", "--shape", "pass-through",
				"--tend-interval", "250", "--timeout", "1000", "--namespace", "test", "--set", "l5",
				"--tls-ca-file", caPem);
			Assert.AreEqual(2, result.Status, result.Stderr);
		}

		[TestMethod]
		public void TlsNameAndCaFileBuildTlsPolicy()
		{
			Driver.Options options = Options("pass-through", "127.0.0.1:3000",
				"--tls-name", TlsName, "--tls-ca-file", caPem);
			Assert.IsNotNull(options.Policy().tlsPolicy);
			Assert.AreEqual(TlsName, options.Seed.tlsName);
			Assert.AreEqual(caPem, options.TlsCaFile);
		}

		private static Driver.Options Options(string shape, string seed, params string[] flags)
		{
			List<string> args = new List<string>
			{
				"--seed", seed, "--shape", shape, "--tend-interval", "250",
				"--timeout", "1000", "--namespace", "test", "--set", "l5"
			};
			args.AddRange(flags);
			return new Driver.Options(args.ToArray(), null);
		}

		private static string PemEncode(byte[] der, string label)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("-----BEGIN ").Append(label).Append("-----\n");
			string b64 = Convert.ToBase64String(der);

			for (int i = 0; i < b64.Length; i += 64)
			{
				sb.Append(b64, i, Math.Min(64, b64.Length - i)).Append('\n');
			}

			sb.Append("-----END ").Append(label).Append("-----\n");
			return sb.ToString();
		}

		private sealed class Result
		{
			public int Status;
			public string Stdout;
			public string Stderr;
		}

		/// <summary>
		/// Run the driver as a process with <paramref name="stdin"/> as its input.
		/// </summary>
		private static Result DriverProcess(string stdin, params string[] args)
		{
			string dll = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..",
				"AerospikeL5Driver", "bin", "Release", "net8.0", "AerospikeL5Driver.dll"));

			if (!File.Exists(dll))
			{
				dll = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..",
					"AerospikeL5Driver", "bin", "Debug", "net8.0", "AerospikeL5Driver.dll"));
			}

			Assert.IsTrue(File.Exists(dll), "AerospikeL5Driver.dll not found; build AerospikeL5Driver first");

			ProcessStartInfo psi = new ProcessStartInfo
			{
				FileName = "dotnet",
				RedirectStandardInput = true,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				UseShellExecute = false
			};
			psi.ArgumentList.Add(dll);

			foreach (string arg in args)
			{
				psi.ArgumentList.Add(arg);
			}

			using (Process process = Process.Start(psi))
			{
				Assert.IsNotNull(process);
				process.StandardInput.Write(stdin);
				process.StandardInput.Close();
				string stdout = process.StandardOutput.ReadToEnd();
				string stderr = process.StandardError.ReadToEnd();

				if (!process.WaitForExit(60000))
				{
					process.Kill(true);
					Assert.Fail("driver did not exit");
				}

				return new Result { Status = process.ExitCode, Stdout = stdout, Stderr = stderr };
			}
		}
	}
}
