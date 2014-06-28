/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using System.Collections.Generic;
using System.IO;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class GetAndJoin : SyncExample
	{
		public GetAndJoin(Console console)
			: base(console)
		{
		}

		/// <summary>
		/// Demonstrate writing bins with replace option. Replace will cause all record bins
		/// to be overwritten.  If an existing bin is not referenced in the replace command,
		/// the bin will be deleted.
		/// <para>
		/// The replace command has a performance advantage over the default put, because 
		/// the server does not have to read the existing record before overwriting it.
		/// </para>
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			// Write securities
			console.Info("Write securities");
			Security security = new Security("GE", 26.89);
			security.Write(client, args.writePolicy, args.ns, args.set);

			security = new Security("IBM", 183.6);
			security.Write(client, args.writePolicy, args.ns, args.set);

			// Write account with positions.
			console.Info("Write account with positions");
			List<Position> positions = new List<Position>(2);
			positions.Add(new Position("GE", 1000));
			positions.Add(new Position("IBM", 500));
			Account accountWrite = new Account("123456", positions);
			accountWrite.Write(client, args.writePolicy, args.ns, args.set);

			// Read account/positions and join with securities.
			console.Info("Read accounts, positions and securities");
			Account accountRead = new Account();
			accountRead.Read(client, args.writePolicy, args.ns, args.set, "123456");

			// Validate data
			accountWrite.Validate(accountRead);
			console.Info("Accounts match");
		}
	}

	class Account
	{
		internal string accountId;
		internal List<Position> positions;

		public Account(string accountId, List<Position> positions)
		{
			this.accountId = accountId;
			this.positions = positions;
		}

		public Account()
		{
		}

		public void Write(AerospikeClient client, WritePolicy policy, string ns, string set)
		{
			Key key = new Key(ns, set, accountId);

			List<object> tickers = new List<object>(positions.Count);
			MemoryStream ms = new MemoryStream(500);
			BinaryWriter writer = new BinaryWriter(ms);
			writer.Write(positions.Count);

			foreach (Position position in positions)
			{
				tickers.Add(position.GetTicker());
				position.Write(writer);
			}
			byte[] positionsBytes = ms.ToArray();

			Bin binPositions = new Bin("positions", positionsBytes);
			Bin binTickers = Bin.AsList("tickers", tickers);

			client.Put(policy, key, binPositions, binTickers);
		}

		public void Read(AerospikeClient client, Policy policy, string ns, string set, string accountId)
		{
			Record record = client.Join(policy,
				new Key(ns, set, accountId),
				new Join("tickers", ns, set));

			if (record == null)
			{
				throw new Exception("Failed to read: " + accountId);
			}

			this.accountId = accountId;
			byte[] positionsBytes = (byte[])record.GetValue("positions");
			Record[] records = (Record[])record.GetValue("tickers");

			if (positionsBytes != null)
			{
				MemoryStream ms = new MemoryStream(positionsBytes);
				BinaryReader reader = new BinaryReader(ms);
				int count = reader.ReadInt32();
				positions = new List<Position>(count);

				for (int i = 0; i < count; i++)
				{
					positions.Add(new Position(reader, records[i]));
				}
			}
			else
			{
				positions = new List<Position>(0);
			}
		}

		public void Validate(Account other)
		{
			if (!this.accountId.Equals(other.accountId))
			{
				throw new Exception("accountId mismatch. Expected " + this.accountId + " Received " + other.accountId);
			}

			if (this.positions.Count != other.positions.Count)
			{
				throw new Exception("positions.Count mismatch. Expected " + this.positions.Count + " Received " + other.positions.Count);
			}

			for (int i = 0; i < this.positions.Count; i++)
			{
				this.positions[i].Validate(other.positions[i]);
			}
		}
	}

	class Position
	{
		internal Security security;
		internal double qty;

		public Position(string ticker, double qty)
		{
			this.security = new Security(ticker, 0.0);
			this.qty = qty;
		}

		public Position(BinaryReader reader, Record record)
		{
			// Read ticker and discard, because the joined record also contains the ticker.
			reader.ReadString();  
			qty = reader.ReadDouble();
			security = new Security(record);
		}

		public void Write(BinaryWriter writer)
		{
			writer.Write(security.ticker);
			writer.Write(qty);
		}

		public string GetTicker()
		{
			return security.ticker;
		}

		public void Validate(Position other)
		{
			this.security.Validate(other.security);
			
			if (this.qty != other.qty)
			{
				throw new Exception("qty mismatch. Expected " + this.qty + " Received " + other.qty);
			}
		}
	}

	class Security
	{
		internal string ticker;
		internal double price;

		public Security(string ticker, double price)
		{
			this.ticker = ticker;
			this.price = price;
		}

		public Security(Record record)
		{
			ticker = (string)record.GetValue("ticker");
			// Convert price double from byte[].
			byte[] priceBytes = (byte[])record.GetValue("price");
			price = BitConverter.ToDouble(priceBytes, 0);
		}

		public void Write(AerospikeClient client, WritePolicy policy, string ns, string set)
		{
			Key key = new Key(ns, set, ticker);
			Bin binTicker = new Bin("ticker", ticker);
			// Double not supported directly, so convert to bytes.
			Bin binPrice = new Bin("price", BitConverter.GetBytes(price));
			client.Put(policy, key, binTicker, binPrice);
		}

		public void Validate(Security other)
		{
			if (!this.ticker.Equals(other.ticker))
			{
				throw new Exception("security.ticker mismatch. Expected " + this.ticker + " Received " + other.ticker);
			}
		}
	}
}
