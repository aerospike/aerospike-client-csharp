using System;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	class MultiThreadedLoad : SyncExample
	{
		private const int UNSET = -1;
		private const int DELETED = -2;
		private const int INUSE = -3;
		
		private static WritePolicy policy;
		private Arguments args;
		private string bin;

		private Thread[] threads;
		private Thread tickerThread;
		private AerospikeClient client;
		private Random random;

		private object valuesLock;
		private int[] values;

		// These are counters available across all threads.
		// Use with Interlocked.increment
		private int writeCounter;
		private int deleteCounter;
		private int readCounter;

		public MultiThreadedLoad(Console console)
			: base(console)
		{
			policy = new WritePolicy();
			policy.timeout = 2000;
		}

		/// <summary>
		/// Write/Read large blocks of data using multiple threads and measure performance.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			this.client = client;
			this.args = args;
			this.bin = args.singleBin ? "" : "demobin";  // Single bin servers don't need a bin name.

			int objectSize = 1000000;
			values = new int[objectSize];

			valuesLock = new Object();
			random = new Random();

			for (int i = 0; i < objectSize; i++)
				values[i] = UNSET;

			writeCounter = 0;
			readCounter = 0;
			deleteCounter = 0;

			StartThreads(30);
		}

		private void StartThreads(int threadMax)
		{
			tickerThread = new Thread(new ThreadStart(this.Ticker));
			tickerThread.Start();

			threads = new Thread[threadMax];
			for (int i = 0; i < threadMax; i++)
			{
				threads[i] = new Thread(new ThreadStart(this.Worker));
				threads[i].Start();
			}

			for (int i = 0; i < threadMax; i++)
			{
				threads[i].Join();
			}
			valid = false;
			tickerThread.Join();
		}

		private void Ticker()
		{
			try
			{
				RunTicker();
			}
			catch (Exception ex)
			{
				console.Error(ex.Message);
			}
		}

		private void RunTicker()
		{
			DateTime prevTime = DateTime.Now;
			int prevTotal = 0;

			while (valid)
			{
				int r = readCounter;
				int w = writeCounter;
				int d = deleteCounter;
				int total = r + w + d;
				DateTime time = DateTime.Now;
				double seconds = time.Subtract(prevTime).TotalSeconds;

				if (seconds > 0.0)
				{
					double opsPerSecond = Math.Round(((double)(total - prevTotal)) / seconds, 0);

					console.Info("Read {0} Write {1} Delete {2} Total {3} Operations/Second {4}",
						r, w, d, total, opsPerSecond);
        
					prevTotal = total;
					prevTime = time;
				}
				Thread.Sleep(1000);
			}
		}

		private void Worker()
		{
			try
			{
				RunWorker();
			}
			catch (Exception ex)
			{
				console.Error(ex.Message);
			}
		}

		private void RunWorker()
		{
			while (valid)
			{
				// Choose key at random.
				int value;
				int key;
				int ops = 0;

				lock (valuesLock)
				{
					do
					{
						if (++ops >= values.Length)
							throw new Exception("All keys in use.");

						key = random.Next(0, values.Length);
					} while (values[key] == INUSE);

					value = values[key];
					values[key] = INUSE;
				}

				try
				{
					// Process corresponding value.
					switch (value)
					{
						case UNSET:
							value = Write(key);
							break;

						case DELETED:
							value = ProcessDeleted(key, value);
							break;

						default:
							value = ProcessNormal(key, value);
							break;
					}
				}
				finally
				{
					lock (valuesLock)
					{
						values[key] = value;
					}
				}
			}
		}

		private int ProcessNormal(int offset, int expected)
		{
			if (Read(offset, expected) == UNSET)
				return UNSET;

			// Roll a die.
			int die;
			lock (valuesLock)
			{
				die = random.Next(0, 3);
			}

			// Delete record 25% of the time.
			if (die == 0)
			{
				return Delete(offset);
			}

			// Update record 50% of the time.
			if (die <= 2)
			{
				return Write(offset);
			}

			// Leave record alone 25% of the time.
			return expected;
		}

		private int ProcessDeleted(int offset, int expected)
		{
			try
			{
				Record record = client.Get(policy, new Key(args.ns, args.set, offset.ToString()), bin);

				if (record != null)
				{
					console.Warn("Read deleted record: ns={0} set={1} key={2} bin={3}",
						args.ns, args.set, offset, bin);
				}
			}
			catch (Exception e)
			{
				console.Warn("Read error: ns={0} set={1} key={2} bin={3} exception={4}",
					args.ns, args.set, offset, bin, e.Message);
			}
			Interlocked.Increment(ref readCounter);

			// Roll a die.
			int die;
			lock (valuesLock)
			{
				die = random.Next(0, 2);
			}

			// Write over deleted value 67% of the time.
			if (die <= 1)
			{
				return Write(offset);
			}

			// Leave record alone 33% of the time.
			return expected;
		}

		private int Write(int key)
		{
			int value;
			lock (valuesLock)
			{
				value = random.Next(0, Int32.MaxValue);
			}

			try
			{
				client.Put(policy, new Key(args.ns, args.set, key.ToString()), new Bin(bin, value.ToString()));
			}
			catch (AerospikeException e)
			{
				if (e.Result != ResultCode.TIMEOUT)
				{
					bool stopWrites = GetIsStopWrites(args.host, args.port, args.ns);

					if (stopWrites)
					{
						if (valid)
						{
							console.Error("Server is currently in readonly mode. Shutting down...");
							valid = false;
						}
					}
					else
					{
						console.Error("Failed to set: ns={0} set={1} key={2} bin={3} value={4} exception={5}",
							args.ns, args.set, key.ToString(), bin, value.ToString(), e.Message);
					}
				}
				return UNSET;
			} 
			Interlocked.Increment(ref writeCounter);
			return value;
		}

		private int Read(int key, int expected)
		{
			object value;
			try
			{
				Record record = client.Get(policy, new Key(args.ns, args.set, key.ToString()), bin);
				value = record.GetValue(bin);
			}
			catch (Exception e)
			{
				console.Warn("Read error: ns={0} set={1} key={2} bin={3} exception={4}",
					args.ns, args.set, key, bin, e.Message);
				return UNSET;
			}

			if (Convert.ToInt32(value) != expected)
			{
				console.Error("Value mismatch: Expected {0}. Received {1}.",
					expected, value);
			}
			Interlocked.Increment(ref readCounter);
			return expected;
		}

		private int Delete(int key)
		{
			try
			{
				client.Delete(policy, new Key(args.ns, args.set, key.ToString()));
			}
			catch (Exception e)
			{
				console.Error("Failed to delete: ns={0} set={1} key={2} exception={3}",
					args.ns, args.set, key.ToString(), e.Message);
				return UNSET;
			}
			Interlocked.Increment(ref deleteCounter);
			return DELETED;
		}

		public bool GetIsStopWrites(string host, int port, string ns)
		{
			string filter = "namespace/" + ns;
			string tokens = Info.Request(host, port, filter);

			if (tokens == null)
			{
				return false;
			}

			string name = "stop-writes";
			string search = name + '=';
			int begin = tokens.IndexOf(search);

			if (begin < 0)
				return false;

			begin += search.Length;
			int end = tokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = tokens.Length;
			}
			string value = tokens.Substring(begin, end - begin);
			return Boolean.Parse(value);
		}
	}
}
