using System;
using System.Collections.Generic;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	abstract class Benchmark
	{
		private const int UNSET = -1;
		private const int DELETED = -2;
		private const int INUSE = -3;
		
		protected static WritePolicy policy;
		protected Console console;

		protected string host;
		protected int port;
		protected string ns;
		protected string setName;
		protected string binName;

		private Thread[] threads;
		private Thread tickerThread;
		private Random random;

		private object valuesLock;
		private int[] values;

		// These counters are available across all threads.
		// Use with Interlocked.increment
		private int writeCount;
		private int writeFailCount;
		private int deleteCount;
		private int deleteFailCount;
		private int readCount;
		private int readFailCount;

		public Benchmark(Console console)
		{
			this.console = console;
			policy = new WritePolicy();
			policy.timeout = 2000;
		}

		/// <summary>
		/// Write/Read large blocks of data using multiple threads and measure performance.
		/// </summary>
		public void RunExample(Arguments args, int threadMax)
		{
			this.ns = args.ns;
			this.setName = args.set;
			this.binName = args.singleBin ? "" : "demobin";  // Single bin servers don't need a bin name.

			int objectSize = 5000000;
			values = new int[objectSize];

			valuesLock = new Object();
			random = new Random();

			for (int i = 0; i < objectSize; i++)
				values[i] = UNSET;

			writeCount = 0;
			writeFailCount = 0;
			readCount = 0;
			readFailCount = 0;
			deleteCount = 0;
			deleteFailCount = 0;

			console.Info("Start " + threadMax + " generator threads");
			StartThreads(threadMax);
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
			SetValid(false);
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

			while (IsValid())
			{
				int writeCurrent = Interlocked.Exchange(ref writeCount, 0);
				int writeFailCurrent = Interlocked.Exchange(ref writeFailCount, 0);
				int readCurrent = Interlocked.Exchange(ref readCount, 0);
				int readFailCurrent = Interlocked.Exchange(ref readFailCount, 0);
				int deleteCurrent = Interlocked.Exchange(ref deleteCount, 0);
				int deleteFailCurrent = Interlocked.Exchange(ref deleteFailCount, 0);
				
				DateTime time = DateTime.Now;
				double seconds = (double)time.Subtract(prevTime).TotalSeconds;
				prevTime = time;

				if (seconds > 0.0)
				{
					double writeTps = Math.Round((double)writeCurrent / seconds, 0);
					double readTps = Math.Round((double)readCurrent / seconds, 0);
					double deleteTps = Math.Round((double)deleteCurrent / seconds, 0);

					console.Info("write(tps={0} fail={1}) read(tps={2} fail={3}) delete(tps={4} fail={5}) total(tps={6} fail={7})",
						writeTps, writeFailCurrent, readTps, readFailCurrent, deleteTps, deleteFailCurrent,
						writeTps + readTps + deleteTps, writeFailCurrent + readFailCurrent + deleteFailCurrent);
        
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
			while (IsValid())
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

				// Process corresponding value.
				switch (value)
				{
					case UNSET:
						Write(key);
						break;

					case DELETED:
						Read(key, value, true);
						break;

					default:
						Read(key, value, false);
						break;
				}
			}
		}

		private void Write(int key)
		{
			int value;
			lock (valuesLock)
			{
				value = random.Next(0, Int32.MaxValue);
			}

			try
			{
				WriteRecord(key, value);
			}
			catch (Exception e)
			{
				OnWriteFailure(key, value, e);
			}
		}

		private void Read(int key, int expected, bool deleted)
		{
			try
			{
				ReadRecord(key, expected, deleted);
			}
			catch (Exception e)
			{
				OnReadFailure(key, e);
			}
		}

		private void Delete(int key)
		{
			try
			{
				DeleteRecord(key);
			}
			catch (AerospikeException e)
			{
				OnDeleteFailure(key, e);
			}
		}

		protected void OnWriteSuccess(int key, int value)
		{
			Interlocked.Increment(ref writeCount);
			SetValue(key, value);
		}

		protected void OnWriteFailure(int key, int value, Exception e)
		{
			Interlocked.Increment(ref writeFailCount);

			if (e is AerospikeException)
			{
				AerospikeException ae = e as AerospikeException;

				if (ae.Result != ResultCode.TIMEOUT)
				{
					bool stopWrites = GetIsStopWrites(host, port, ns);

					if (stopWrites)
					{
						if (IsValid())
						{
							console.Error("Server is currently in readonly mode. Shutting down...");
							SetValid(false);
						}
					}
					else
					{
						console.Error("Failed to set: ns={0} set={1} key={2} bin={3} value={4} exception={5}",
							ns, setName, key, binName, value, e.Message);
					}
				}
			}
			else
			{
				console.Error("Failed to set: ns={0} set={1} key={2} bin={3} value={4} exception={5}",
					ns, setName, key, binName, value, e.Message);
			}
			SetValue(key, UNSET);
		}

		protected void OnReadSuccess(int key, int expected, bool deleted, Record record)
		{
			if (deleted)
			{
				if (record == null)
				{
					// Success, deleted record not found.
					CreateNextDeleteAction(key);
				}
				else
				{
					// Failure. deleted record still exists.
					Interlocked.Increment(ref readFailCount);
					console.Warn("Read deleted record: ns={0} set={1} key={2} bin={3}",
						ns, setName, key, binName);
					SetValue(key, UNSET);
				}
			}
			else
			{
				if (record == null)
				{
					// Failure, record no longer exists.
					OnReadFailure(key, "Record not found");
				}
				else
				{
					object obj = record.GetValue(binName);
					int value = Convert.ToInt32(obj);

					if (value == expected)
					{
						SetValue(key, expected);
						CreateNextAction(key);
					}
					else
					{
						Interlocked.Increment(ref readFailCount);
						console.Error("Value mismatch: Expected {0}. Received {1}.",
							expected, value);
						SetValue(key, UNSET);
					}
				}
			}
		}

		protected void OnReadFailure(int key, Exception e)
		{
			OnReadFailure(key, e.Message);
		}

		protected void OnReadFailure(int key, string message)
		{
			Interlocked.Increment(ref readFailCount);
			console.Warn("Read error: ns={0} set={1} key={2} bin={3} exception={4}",
				ns, setName, key, binName, message);
			SetValue(key, UNSET);
		}

		protected void OnDeleteSuccess(int key)
		{
			Interlocked.Increment(ref deleteCount);
			SetValue(key, DELETED);
		}

		protected void OnDeleteFailure(int key, AerospikeException e)
		{
			Interlocked.Increment(ref deleteFailCount);
			console.Error("Failed to delete: ns={0} set={1} key={2} exception={3}",
				ns, setName, key, e.Message);
			SetValue(key, UNSET);
		}

		private void CreateNextAction(int key)
		{
			Interlocked.Increment(ref readCount);

			// Roll a die.
			int die;
			lock (valuesLock)
			{
				die = random.Next(0, 3);
			}

			// Delete record 25% of the time.
			if (die == 0)
			{
				Delete(key);
			}

			// Update record 50% of the time.
			if (die <= 2)
			{
				Write(key);
			}

			// Leave record alone 25% of the time.
		}

		private void CreateNextDeleteAction(int key)
		{
			Interlocked.Increment(ref readCount);

			// Roll a die.
			int die;
			lock (valuesLock)
			{
				die = random.Next(0, 2);
			}

			// Write over deleted value 67% of the time.
			if (die <= 1)
			{
				Write(key);
			}

			// Leave record alone 33% of the time.
		}

		private void SetValue(int key, int value)
		{
			lock (valuesLock)
			{
				values[key] = value;
			}
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

		protected abstract void WriteRecord(int key, int value);
		protected abstract void ReadRecord(int key, int expected, bool deleted);
		protected abstract void DeleteRecord(int key);
		protected abstract bool IsValid();
		protected abstract void SetValid(bool valid);
	}
}
