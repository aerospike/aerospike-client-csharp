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
namespace Aerospike.Client
{
	public sealed class BatchAttr
	{
		public Expression filterExp;
		public int readAttr;
		public int writeAttr;
		public int infoAttr;
		public int expiration;
		public short generation;
		public bool hasWrite;
		public bool sendKey;

		public BatchAttr()
		{
		}

		public BatchAttr(Policy policy, int rattr)
		{
			SetRead(policy);
			this.readAttr |= rattr;
		}

		public BatchAttr(Policy policy, int rattr, Operation[] ops)
		{
			SetRead(policy);
			this.readAttr |= rattr;

			if (ops != null)
			{
				AdjustRead(ops);
			}
		}

		public BatchAttr(BatchPolicy rp, BatchWritePolicy wp, Operation[] ops)
		{
			bool readAllBins = false;
			bool readHeader = false;
			bool hasRead = false;
			bool hasWriteOp = false;

			foreach (Operation op in ops)
			{
				if (Operation.IsWrite(op.type))
				{
					hasWriteOp = true;
				}
				else
				{
					hasRead = true;

					if (op.type == Operation.Type.READ)
					{
						if (op.binName == null)
						{
							readAllBins = true;
						}
					}
					else if (op.type == Operation.Type.READ_HEADER)
					{
						readHeader = true;
					}
				}
			}

			if (hasWriteOp)
			{
				SetWrite(wp);

				if (hasRead)
				{
					readAttr |= Command.INFO1_READ;

					if (readAllBins)
					{
						readAttr |= Command.INFO1_GET_ALL;
						// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
						writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
					}
					else if (readHeader)
					{
						readAttr |= Command.INFO1_NOBINDATA;
					}
				}
			}
			else
			{
				SetRead(rp);

				if (readAllBins)
				{
					readAttr |= Command.INFO1_GET_ALL;
				}
				else if (readHeader)
				{
					readAttr |= Command.INFO1_NOBINDATA;
				}
			}
		}

		public void SetRead(Policy rp)
		{
			filterExp = null;
			readAttr = Command.INFO1_READ;

			if (rp.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			writeAttr = 0;

			switch (rp.readModeSC)
			{
				default:
					infoAttr = 0;
					break;
				case ReadModeSC.SESSION:
					infoAttr = 0;
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr = Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr = Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr = Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}
			expiration = 0;
			generation = 0;
			hasWrite = false;
			sendKey = false;
		}

		public void SetRead(BatchReadPolicy rp)
		{
			filterExp = rp.filterExp;
			readAttr = Command.INFO1_READ;

			if (rp.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			writeAttr = 0;

			switch (rp.readModeSC)
			{
				default:
					infoAttr = 0;
					break;
				case ReadModeSC.SESSION:
					infoAttr = 0;
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr = Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr = Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr = Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}
			expiration = 0;
			generation = 0;
			hasWrite = false;
			sendKey = false;
		}

		public void AdjustRead(Operation[] ops)
		{
			foreach (Operation op in ops)
			{
				if (op.type == Operation.Type.READ)
				{
					if (op.binName == null)
					{
						readAttr |= Command.INFO1_GET_ALL;
					}
				}
				else if (op.type == Operation.Type.READ_HEADER)
				{
					readAttr |= Command.INFO1_NOBINDATA;
				}
			}
		}

		public void AdjustRead(bool readAllBins)
		{
			if (readAllBins)
			{
				readAttr |= Command.INFO1_GET_ALL;
			}
			else
			{
				readAttr |= Command.INFO1_NOBINDATA;
			}
		}

		public void SetWrite(Policy wp)
		{
			filterExp = null;
			readAttr = 0;
			writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS;
			infoAttr = 0;
			expiration = 0;
			generation = 0;
			hasWrite = true;
			sendKey = wp.sendKey;
		}

		public void SetWrite(BatchWritePolicy wp)
		{
			filterExp = wp.filterExp;
			readAttr = 0;
			writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS;
			infoAttr = 0;
			expiration = wp.expiration;
			hasWrite = true;
			sendKey = wp.sendKey;

			switch (wp.generationPolicy)
			{
				default:
					generation = 0;
					break;
				case GenerationPolicy.NONE:
					generation = 0;
					break;
				case GenerationPolicy.EXPECT_GEN_EQUAL:
					generation = (short)wp.generation;
					writeAttr |= Command.INFO2_GENERATION;
					break;
				case GenerationPolicy.EXPECT_GEN_GT:
					generation = (short)wp.generation;
					writeAttr |= Command.INFO2_GENERATION_GT;
					break;
			}

			switch (wp.recordExistsAction)
			{
				case RecordExistsAction.UPDATE:
					break;
				case RecordExistsAction.UPDATE_ONLY:
					infoAttr |= Command.INFO3_UPDATE_ONLY;
					break;
				case RecordExistsAction.REPLACE:
					infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
					break;
				case RecordExistsAction.REPLACE_ONLY:
					infoAttr |= Command.INFO3_REPLACE_ONLY;
					break;
				case RecordExistsAction.CREATE_ONLY:
					writeAttr |= Command.INFO2_CREATE_ONLY;
					break;
			}

			if (wp.durableDelete)
			{
				writeAttr |= Command.INFO2_DURABLE_DELETE;
			}

			if (wp.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= Command.INFO3_COMMIT_MASTER;
			}
		}

		public void AdjustWrite(Operation[] ops)
		{
			foreach (Operation op in ops)
			{
				if (! Operation.IsWrite(op.type))
				{
					readAttr |= Command.INFO1_READ;

					if (op.type == Operation.Type.READ)
					{
						if (op.binName == null)
						{
							readAttr |= Command.INFO1_GET_ALL;
							// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
							writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
						}
					}
					else if (op.type == Operation.Type.READ_HEADER)
					{
						readAttr |= Command.INFO1_NOBINDATA;
					}
				}
			}
		}

		public void SetUDF(Policy up)
		{
			filterExp = null;
			readAttr = 0;
			writeAttr = Command.INFO2_WRITE;
			infoAttr = 0;
			expiration = 0;
			generation = 0;
			hasWrite = true;
			sendKey = up.sendKey;
		}

		public void SetUDF(BatchUDFPolicy up)
		{
			filterExp = up.filterExp;
			readAttr = 0;
			writeAttr = Command.INFO2_WRITE;
			infoAttr = 0;
			expiration = up.expiration;
			generation = 0;
			hasWrite = true;
			sendKey = up.sendKey;

			if (up.durableDelete)
			{
				writeAttr |= Command.INFO2_DURABLE_DELETE;
			}

			if (up.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= Command.INFO3_COMMIT_MASTER;
			}
		}

		public void SetDelete(Policy dp)
		{
			filterExp = null;
			readAttr = 0;
			writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DELETE;
			infoAttr = 0;
			expiration = 0;
			generation = 0;
			hasWrite = true;
			sendKey = dp.sendKey;
		}

		public void SetDelete(BatchDeletePolicy dp)
		{
			filterExp = dp.filterExp;
			readAttr = 0;
			writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DELETE;
			infoAttr = 0;
			expiration = 0;
			hasWrite = true;
			sendKey = dp.sendKey;

			switch (dp.generationPolicy)
			{
				default:
					generation = 0;
					break;
				case GenerationPolicy.NONE:
					generation = 0;
					break;
				case GenerationPolicy.EXPECT_GEN_EQUAL:
					generation = (short)dp.generation;
					writeAttr |= Command.INFO2_GENERATION;
					break;
				case GenerationPolicy.EXPECT_GEN_GT:
					generation = (short)dp.generation;
					writeAttr |= Command.INFO2_GENERATION_GT;
					break;
			}

			if (dp.durableDelete)
			{
				writeAttr |= Command.INFO2_DURABLE_DELETE;
			}

			if (dp.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= Command.INFO3_COMMIT_MASTER;
			}
		}
	}
}
