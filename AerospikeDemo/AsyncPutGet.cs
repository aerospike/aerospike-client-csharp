using System;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class AsyncPutGet : AsyncExample
	{
		private bool completed;

		public AsyncPutGet(Console console) : base(console)
		{
		}

		/// <summary>
		/// Asynchronously write and read a bin using alternate methods.
		/// </summary>
		public override void RunExample(AsyncClient client, Arguments args)
		{
			completed = false;
			Key key = new Key(args.ns, args.set, "putgetkey");
			Bin bin = new Bin(args.GetBinName("putgetbin"), "value");

			RunPutGet(client, args, key, bin);
			WaitTillComplete();
		}

		private void RunPutGet(AsyncClient client, Arguments args, Key key, Bin bin)
		{
			console.Info("Put: namespace={0} set={1} key={2} value={3}", 
				key.ns, key.setName, key.userKey, bin.value);

			client.Put(args.writePolicy, new WriteHandler(this, client, args.writePolicy, key, bin), key, bin);
		}

		private class WriteHandler : WriteListener
		{
			private readonly AsyncPutGet parent;
			private AsyncClient client;
			private WritePolicy policy;
			private Key key;
			private Bin bin;

			public WriteHandler(AsyncPutGet parent, AsyncClient client, WritePolicy policy, Key key, Bin bin)
			{
				this.parent = parent;
				this.client = client;
				this.policy = policy;
				this.key = key;
				this.bin = bin;
			}

			public void OnSuccess(Key key)
			{
				try
				{
					// Write succeeded.  Now call read.
					parent.console.Info("Get: namespace={0} set={1} key={2}", 
						key.ns, key.setName, key.userKey);

					client.Get(policy, new RecordHandler(parent, key, bin), key);
				}
				catch (Exception e)
				{
					parent.console.Error("Failed to get: namespace={0} set={1} key={2} exception={3}", 
						key.ns, key.setName, key.userKey, e.Message);
				}
			}

			public void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to put: namespace={0} set={1} key={2} exception={3}", 
					key.ns, key.setName, key.userKey, e.Message);

				parent.NotifyCompleted();
			}
		}

		private class RecordHandler : RecordListener
		{
			private readonly AsyncPutGet parent;
			private Key key;
			private Bin bin;

			public RecordHandler(AsyncPutGet parent, Key key, Bin bin)
			{
				this.parent = parent;
				this.key = key;
				this.bin = bin;
			}

			public virtual void OnSuccess(Key key, Record record)
			{
				parent.ValidateBin(key, bin, record);
				parent.NotifyCompleted();
			}

			public virtual void OnFailure(AerospikeException e)
			{
				parent.console.Error("Failed to get: namespace={0} set={1} key={2} exception={3}",
					key.ns, key.setName, key.userKey, e.Message);

				parent.NotifyCompleted();
			}
		}

		private void ValidateBin(Key key, Bin bin, Record record)
		{
			object received = (record == null)? null : record.GetValue(bin.name);
			string expected = bin.value.ToString();

			if (received != null && received.Equals(expected))
			{
				console.Info("Bin matched: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Put/Get mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}

		private void WaitTillComplete()
		{
			lock (this)
			{
				while (!completed)
				{
					Monitor.Wait(this);
				}
			}
		}

		private void NotifyCompleted()
		{
			lock (this)
			{
				completed = true;
				Monitor.Pulse(this);
			}
		}
	}
}