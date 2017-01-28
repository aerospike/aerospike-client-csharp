/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
/* 
 * Copyright (c) 2000 - 2015 The Legion of the Bouncy Castle Inc. (http://www.bouncycastle.org)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
using System;

namespace Aerospike.Client
{
	/// <summary>
	/// RIPEMD-160 hash.  Algorithm provided by Hans Dobbertin, Antoon Bosselaers, and Bart Preneel:
	/// https://homes.esat.kuleuven.be/~bosselae/ripemd160.html
	/// 
	/// This is an optimized implementation based on Bouncy Castle's RipeMD160Digest.cs:
	/// http://www.bouncycastle.org/csharp
	/// </summary>
	public sealed class Ripemd160
	{
		private uint[] X = new uint[16];
		private byte[] xBuf = new byte[4];
		private int xOff;
		private int xBufOff;
		private long byteCount;
		private uint H0 = 0x67452301;
		private uint H1 = 0xefcdab89;
		private uint H2 = 0x98badcfe;
		private uint H3 = 0x10325476;
		private uint H4 = 0xc3d2e1f0;

		/// <summary>
		/// Reset parameters to start new hash.
		/// Only call if plan to hash multiple times with same Ripemd160 instance.
		/// Do not call if only performing one hash per Ripemd160 instance.
		/// </summary>
		public void Reset()
		{
			Array.Clear(X, 0, X.Length);
			Array.Clear(xBuf, 0, xBuf.Length);
			xOff = 0;
			xBufOff = 0;
			byteCount = 0;
			H0 = 0x67452301;
			H1 = 0xefcdab89;
			H2 = 0x98badcfe;
			H3 = 0x10325476;
			H4 = 0xc3d2e1f0;
		}

		/// <summary>
		/// Add bytes to be hashed.
		/// </summary>
		public void Add(byte[] input, int inOff, int length)
		{
			// fill the current word
			int i = 0;
			if (xBufOff != 0)
			{
				while (i < length)
				{
					xBuf[xBufOff++] = input[inOff + i++];
					if (xBufOff == 4)
					{
						ProcessWord(xBuf, 0);
						xBufOff = 0;
						break;
					}
				}
			}

			// process whole words.
			int limit = ((length - i) & ~3) + i;
			for (; i < limit; i += 4)
			{
				ProcessWord(input, inOff + i);
			}

			// load in the remainder.
			while (i < length)
			{
				xBuf[xBufOff++] = input[inOff + i++];
			}

			byteCount += length;
		}

		/// <summary>
		/// Add byte to be hashed.
		/// </summary>
		public void Add(byte input)
		{
			xBuf[xBufOff++] = input;

			if (xBufOff == xBuf.Length)
			{
				ProcessWord(xBuf, 0);
				xBufOff = 0;
			}

			byteCount++;
		}

		/// <summary>
		/// Return hash digest for bytes that have been added up to this point.
		/// </summary>
		public byte[] HashDigest()
        {
            long bitLength = (byteCount << 3);

            // add the pad bytes.
            Add((byte)128);

			while (xBufOff != 0)
			{
				Add((byte)0);
			}
            ProcessLength(bitLength);
            ProcessBlock();

			return new byte[]
			{
				(byte)H0,
				(byte)(H0 >> 8),
				(byte)(H0 >> 16),
				(byte)(H0 >> 24),
				(byte)H1,
				(byte)(H1 >> 8),
				(byte)(H1 >> 16),
				(byte)(H1 >> 24),
				(byte)H2,
				(byte)(H2 >> 8),
				(byte)(H2 >> 16),
				(byte)(H2 >> 24),
				(byte)H3,
				(byte)(H3 >> 8),
				(byte)(H3 >> 16),
				(byte)(H3 >> 24),
				(byte)H4,
				(byte)(H4 >> 8),
				(byte)(H4 >> 16),
				(byte)(H4 >> 24)
			};
        }

		private void ProcessWord(byte[] input, int inOff)
		{
			X[xOff++] = ((uint)input[inOff]) |
						(((uint)input[inOff + 1]) << 8) | 
						(((uint)input[inOff + 2]) << 16) | 
						(((uint)input[inOff + 3]) << 24);

			if (xOff == 16)
			{
				ProcessBlock();
			}
		}

		private void ProcessLength(long bitLength)
		{
			if (xOff > 14)
			{
				ProcessBlock();
			}

			X[14] = (uint)(bitLength & 0xffffffff);
			X[15] = (uint)((ulong)bitLength >> 32);
		}
		
		private void ProcessBlock()
		{
			uint aa = H0, aaa = H0;
			uint bb = H1, bbb = H1;
			uint cc = H2, ccc = H2;
			uint dd = H3, ddd = H3;
			uint ee = H4, eee = H4;

			// Unwind C reference macros instead of making function calls.
			// This implementation is twice as fast as the Bouncy Castle implementation.

			// round 1
			// FF(aa, bb, cc, dd, ee, X[ 0], 11);
			aa += (bb ^ cc ^ dd) + X[0];
			aa = ((aa << 11) | (aa >> (32 - 11))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// FF(ee, aa, bb, cc, dd, X[ 1], 14);
			ee += (aa ^ bb ^ cc) + X[1];
			ee = ((ee << 14) | (ee >> (32 - 14))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// FF(dd, ee, aa, bb, cc, X[ 2], 15);
			dd += (ee ^ aa ^ bb) + X[2];
			dd = ((dd << 15) | (dd >> (32 - 15))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// FF(cc, dd, ee, aa, bb, X[ 3], 12);
			cc += (dd ^ ee ^ aa) + X[3];
			cc = ((cc << 12) | (cc >> (32 - 12))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// FF(bb, cc, dd, ee, aa, X[ 4],  5);
			bb += (cc ^ dd ^ ee) + X[4];
			bb = ((bb << 5) | (bb >> (32 - 5))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// FF(aa, bb, cc, dd, ee, X[ 5],  8);
			aa += (bb ^ cc ^ dd) + X[5];
			aa = ((aa << 8) | (aa >> (32 - 8))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// FF(ee, aa, bb, cc, dd, X[ 6],  7);
			ee += (aa ^ bb ^ cc) + X[6];
			ee = ((ee << 7) | (ee >> (32 - 7))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// FF(dd, ee, aa, bb, cc, X[ 7],  9);
			dd += (ee ^ aa ^ bb) + X[7];
			dd = ((dd << 9) | (dd >> (32 - 9))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// FF(cc, dd, ee, aa, bb, X[ 8], 11);
			cc += (dd ^ ee ^ aa) + X[8];
			cc = ((cc << 11) | (cc >> (32 - 11))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// FF(bb, cc, dd, ee, aa, X[ 9], 13);
			bb += (cc ^ dd ^ ee) + X[9];
			bb = ((bb << 13) | (bb >> (32 - 13))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// FF(aa, bb, cc, dd, ee, X[10], 14);
			aa += (bb ^ cc ^ dd) + X[10];
			aa = ((aa << 14) | (aa >> (32 - 14))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// FF(ee, aa, bb, cc, dd, X[11], 15);
			ee += (aa ^ bb ^ cc) + X[11];
			ee = ((ee << 15) | (ee >> (32 - 15))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// FF(dd, ee, aa, bb, cc, X[12],  6);
			dd += (ee ^ aa ^ bb) + X[12];
			dd = ((dd << 6) | (dd >> (32 - 6))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// FF(cc, dd, ee, aa, bb, X[13],  7);
			cc += (dd ^ ee ^ aa) + X[13];
			cc = ((cc << 7) | (cc >> (32 - 7))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// FF(bb, cc, dd, ee, aa, X[14],  9);
			bb += (cc ^ dd ^ ee) + X[14];
			bb = ((bb << 9) | (bb >> (32 - 9))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// FF(aa, bb, cc, dd, ee, X[15],  8);
			aa += (bb ^ cc ^ dd) + X[15];
			aa = ((aa << 8) | (aa >> (32 - 8))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// round 2
			// GG(ee, aa, bb, cc, dd, X[ 7],  7);
			ee += ((aa & bb) | (~aa & cc)) + X[7] + 0x5a827999;
			ee = ((ee << 7) | (ee >> (32 - 7))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// GG(dd, ee, aa, bb, cc, X[ 4],  6);
			dd += ((ee & aa) | (~ee & bb)) + X[4] + 0x5a827999;
			dd = ((dd << 6) | (dd >> (32 - 6))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// GG(cc, dd, ee, aa, bb, X[13],  8);
			cc += ((dd & ee) | (~dd & aa)) + X[13] + 0x5a827999;
			cc = ((cc << 8) | (cc >> (32 - 8))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// GG(bb, cc, dd, ee, aa, X[ 1], 13);
			bb += ((cc & dd) | (~cc & ee)) + X[1] + 0x5a827999;
			bb = ((bb << 13) | (bb >> (32 - 13))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// GG(aa, bb, cc, dd, ee, X[10], 11);
			aa += ((bb & cc) | (~bb & dd)) + X[10] + 0x5a827999;
			aa = ((aa << 11) | (aa >> (32 - 11))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// GG(ee, aa, bb, cc, dd, X[ 6],  9);
			ee += ((aa & bb) | (~aa & cc)) + X[6] + 0x5a827999;
			ee = ((ee << 9) | (ee >> (32 - 9))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// GG(dd, ee, aa, bb, cc, X[15],  7);
			dd += ((ee & aa) | (~ee & bb)) + X[15] + 0x5a827999;
			dd = ((dd << 7) | (dd >> (32 - 7))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// GG(cc, dd, ee, aa, bb, X[ 3], 15);
			cc += ((dd & ee) | (~dd & aa)) + X[3] + 0x5a827999;
			cc = ((cc << 15) | (cc >> (32 - 15))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// GG(bb, cc, dd, ee, aa, X[12],  7);
			bb += ((cc & dd) | (~cc & ee)) + X[12] + 0x5a827999;
			bb = ((bb << 7) | (bb >> (32 - 7))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// GG(aa, bb, cc, dd, ee, X[ 0], 12);
			aa += ((bb & cc) | (~bb & dd)) + X[0] + 0x5a827999;
			aa = ((aa << 12) | (aa >> (32 - 12))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// GG(ee, aa, bb, cc, dd, X[ 9], 15);
			ee += ((aa & bb) | (~aa & cc)) + X[9] + 0x5a827999;
			ee = ((ee << 15) | (ee >> (32 - 15))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// GG(dd, ee, aa, bb, cc, X[ 5],  9);
			dd += ((ee & aa) | (~ee & bb)) + X[5] + 0x5a827999;
			dd = ((dd << 9) | (dd >> (32 - 9))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// GG(cc, dd, ee, aa, bb, X[ 2], 11);
			cc += ((dd & ee) | (~dd & aa)) + X[2] + 0x5a827999;
			cc = ((cc << 11) | (cc >> (32 - 11))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// GG(bb, cc, dd, ee, aa, X[14],  7);
			bb += ((cc & dd) | (~cc & ee)) + X[14] + 0x5a827999;
			bb = ((bb << 7) | (bb >> (32 - 7))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// GG(aa, bb, cc, dd, ee, X[11], 13);
			aa += ((bb & cc) | (~bb & dd)) + X[11] + 0x5a827999;
			aa = ((aa << 13) | (aa >> (32 - 13))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// GG(ee, aa, bb, cc, dd, X[ 8], 12);
			ee += ((aa & bb) | (~aa & cc)) + X[8] + 0x5a827999;
			ee = ((ee << 12) | (ee >> (32 - 12))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// round 3
			// HH(dd, ee, aa, bb, cc, X[ 3], 11);
			dd += ((ee | ~aa) ^ bb) + X[3] + 0x6ed9eba1;
			dd = ((dd << 11) | (dd >> (32 - 11))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// HH(cc, dd, ee, aa, bb, X[10], 13);
			cc += ((dd | ~ee) ^ aa) + X[10] + 0x6ed9eba1;
			cc = ((cc << 13) | (cc >> (32 - 13))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// HH(bb, cc, dd, ee, aa, X[14],  6);
			bb += ((cc | ~dd) ^ ee) + X[14] + 0x6ed9eba1;
			bb = ((bb << 6) | (bb >> (32 - 6))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// HH(aa, bb, cc, dd, ee, X[ 4],  7);
			aa += ((bb | ~cc) ^ dd) + X[4] + 0x6ed9eba1;
			aa = ((aa << 7) | (aa >> (32 - 7))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// HH(ee, aa, bb, cc, dd, X[ 9], 14);
			ee += ((aa | ~bb) ^ cc) + X[9] + 0x6ed9eba1;
			ee = ((ee << 14) | (ee >> (32 - 14))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// HH(dd, ee, aa, bb, cc, X[15],  9);
			dd += ((ee | ~aa) ^ bb) + X[15] + 0x6ed9eba1;
			dd = ((dd << 9) | (dd >> (32 - 9))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// HH(cc, dd, ee, aa, bb, X[ 8], 13);
			cc += ((dd | ~ee) ^ aa) + X[8] + 0x6ed9eba1;
			cc = ((cc << 13) | (cc >> (32 - 13))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// HH(bb, cc, dd, ee, aa, X[ 1], 15);
			bb += ((cc | ~dd) ^ ee) + X[1] + 0x6ed9eba1;
			bb = ((bb << 15) | (bb >> (32 - 15))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// HH(aa, bb, cc, dd, ee, X[ 2], 14);
			aa += ((bb | ~cc) ^ dd) + X[2] + 0x6ed9eba1;
			aa = ((aa << 14) | (aa >> (32 - 14))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// HH(ee, aa, bb, cc, dd, X[ 7],  8);
			ee += ((aa | ~bb) ^ cc) + X[7] + 0x6ed9eba1;
			ee = ((ee << 8) | (ee >> (32 - 8))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// HH(dd, ee, aa, bb, cc, X[ 0], 13);
			dd += ((ee | ~aa) ^ bb) + X[0] + 0x6ed9eba1;
			dd = ((dd << 13) | (dd >> (32 - 13))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// HH(cc, dd, ee, aa, bb, X[ 6],  6);
			cc += ((dd | ~ee) ^ aa) + X[6] + 0x6ed9eba1;
			cc = ((cc << 6) | (cc >> (32 - 6))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// HH(bb, cc, dd, ee, aa, X[13],  5);
			bb += ((cc | ~dd) ^ ee) + X[13] + 0x6ed9eba1;
			bb = ((bb << 5) | (bb >> (32 - 5))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// HH(aa, bb, cc, dd, ee, X[11], 12);
			aa += ((bb | ~cc) ^ dd) + X[11] + 0x6ed9eba1;
			aa = ((aa << 12) | (aa >> (32 - 12))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// HH(ee, aa, bb, cc, dd, X[ 5],  7);
			ee += ((aa | ~bb) ^ cc) + X[5] + 0x6ed9eba1;
			ee = ((ee << 7) | (ee >> (32 - 7))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// HH(dd, ee, aa, bb, cc, X[12],  5);
			dd += ((ee | ~aa) ^ bb) + X[12] + 0x6ed9eba1;
			dd = ((dd << 5) | (dd >> (32 - 5))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// round 4
			// II(cc, dd, ee, aa, bb, X[ 1], 11);
			cc += ((dd & aa) | (ee & ~aa)) + X[1] + 0x8f1bbcdc;
			cc = ((cc << 11) | (cc >> (32 - 11))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// II(bb, cc, dd, ee, aa, X[ 9], 12);
			bb += ((cc & ee) | (dd & ~ee)) + X[9] + 0x8f1bbcdc;
			bb = ((bb << 12) | (bb >> (32 - 12))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// II(aa, bb, cc, dd, ee, X[11], 14);
			aa += ((bb & dd) | (cc & ~dd)) + X[11] + 0x8f1bbcdc;
			aa = ((aa << 14) | (aa >> (32 - 14))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// II(ee, aa, bb, cc, dd, X[10], 15);
			ee += ((aa & cc) | (bb & ~cc)) + X[10] + 0x8f1bbcdc;
			ee = ((ee << 15) | (ee >> (32 - 15))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// II(dd, ee, aa, bb, cc, X[ 0], 14);
			dd += ((ee & bb) | (aa & ~bb)) + X[0] + 0x8f1bbcdc;
			dd = ((dd << 14) | (dd >> (32 - 14))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// II(cc, dd, ee, aa, bb, X[ 8], 15);
			cc += ((dd & aa) | (ee & ~aa)) + X[8] + 0x8f1bbcdc;
			cc = ((cc << 15) | (cc >> (32 - 15))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// II(bb, cc, dd, ee, aa, X[12],  9);
			bb += ((cc & ee) | (dd & ~ee)) + X[12] + 0x8f1bbcdc;
			bb = ((bb << 9) | (bb >> (32 - 9))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// II(aa, bb, cc, dd, ee, X[ 4],  8);
			aa += ((bb & dd) | (cc & ~dd)) + X[4] + 0x8f1bbcdc;
			aa = ((aa << 8) | (aa >> (32 - 8))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// II(ee, aa, bb, cc, dd, X[13],  9);
			ee += ((aa & cc) | (bb & ~cc)) + X[13] + 0x8f1bbcdc;
			ee = ((ee << 9) | (ee >> (32 - 9))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// II(dd, ee, aa, bb, cc, X[ 3], 14);
			dd += ((ee & bb) | (aa & ~bb)) + X[3] + 0x8f1bbcdc;
			dd = ((dd << 14) | (dd >> (32 - 14))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// II(cc, dd, ee, aa, bb, X[ 7],  5);
			cc += ((dd & aa) | (ee & ~aa)) + X[7] + 0x8f1bbcdc;
			cc = ((cc << 5) | (cc >> (32 - 5))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// II(bb, cc, dd, ee, aa, X[15],  6);
			bb += ((cc & ee) | (dd & ~ee)) + X[15] + 0x8f1bbcdc;
			bb = ((bb << 6) | (bb >> (32 - 6))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// II(aa, bb, cc, dd, ee, X[14],  8);
			aa += ((bb & dd) | (cc & ~dd)) + X[14] + 0x8f1bbcdc;
			aa = ((aa << 8) | (aa >> (32 - 8))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// II(ee, aa, bb, cc, dd, X[ 5],  6);
			ee += ((aa & cc) | (bb & ~cc)) + X[5] + 0x8f1bbcdc;
			ee = ((ee << 6) | (ee >> (32 - 6))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// II(dd, ee, aa, bb, cc, X[ 6],  5);
			dd += ((ee & bb) | (aa & ~bb)) + X[6] + 0x8f1bbcdc;
			dd = ((dd << 5) | (dd >> (32 - 5))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// II(cc, dd, ee, aa, bb, X[ 2], 12);
			cc += ((dd & aa) | (ee & ~aa)) + X[2] + 0x8f1bbcdc;
			cc = ((cc << 12) | (cc >> (32 - 12))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// round 5
			// JJ(bb, cc, dd, ee, aa, X[ 4],  9);
			bb += (cc ^ (dd | ~ee)) + X[4] + 0xa953fd4e;
			bb = ((bb << 9) | (bb >> (32 - 9))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// JJ(aa, bb, cc, dd, ee, X[ 0], 15);
			aa += (bb ^ (cc | ~dd)) + X[0] + 0xa953fd4e;
			aa = ((aa << 15) | (aa >> (32 - 15))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// JJ(ee, aa, bb, cc, dd, X[ 5],  5);
			ee += (aa ^ (bb | ~cc)) + X[5] + 0xa953fd4e;
			ee = ((ee << 5) | (ee >> (32 - 5))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// JJ(dd, ee, aa, bb, cc, X[ 9], 11);
			dd += (ee ^ (aa | ~bb)) + X[9] + 0xa953fd4e;
			dd = ((dd << 11) | (dd >> (32 - 11))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// JJ(cc, dd, ee, aa, bb, X[ 7],  6);
			cc += (dd ^ (ee | ~aa)) + X[7] + 0xa953fd4e;
			cc = ((cc << 6) | (cc >> (32 - 6))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// JJ(bb, cc, dd, ee, aa, X[12],  8);
			bb += (cc ^ (dd | ~ee)) + X[12] + 0xa953fd4e;
			bb = ((bb << 8) | (bb >> (32 - 8))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// JJ(aa, bb, cc, dd, ee, X[ 2], 13);
			aa += (bb ^ (cc | ~dd)) + X[2] + 0xa953fd4e;
			aa = ((aa << 13) | (aa >> (32 - 13))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// JJ(ee, aa, bb, cc, dd, X[10], 12);
			ee += (aa ^ (bb | ~cc)) + X[10] + 0xa953fd4e;
			ee = ((ee << 12) | (ee >> (32 - 12))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// JJ(dd, ee, aa, bb, cc, X[14],  5);
			dd += (ee ^ (aa | ~bb)) + X[14] + 0xa953fd4e;
			dd = ((dd << 5) | (dd >> (32 - 5))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// JJ(cc, dd, ee, aa, bb, X[ 1], 12);
			cc += (dd ^ (ee | ~aa)) + X[1] + 0xa953fd4e;
			cc = ((cc << 12) | (cc >> (32 - 12))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// JJ(bb, cc, dd, ee, aa, X[ 3], 13);
			bb += (cc ^ (dd | ~ee)) + X[3] + 0xa953fd4e;
			bb = ((bb << 13) | (bb >> (32 - 13))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// JJ(aa, bb, cc, dd, ee, X[ 8], 14);
			aa += (bb ^ (cc | ~dd)) + X[8] + 0xa953fd4e;
			aa = ((aa << 14) | (aa >> (32 - 14))) + ee;
			cc = ((cc << 10) | (cc >> (32 - 10)));

			// JJ(ee, aa, bb, cc, dd, X[11], 11);
			ee += (aa ^ (bb | ~cc)) + X[11] + 0xa953fd4e;
			ee = ((ee << 11) | (ee >> (32 - 11))) + dd;
			bb = ((bb << 10) | (bb >> (32 - 10)));

			// JJ(dd, ee, aa, bb, cc, X[ 6],  8);
			dd += (ee ^ (aa | ~bb)) + X[6] + 0xa953fd4e;
			dd = ((dd << 8) | (dd >> (32 - 8))) + cc;
			aa = ((aa << 10) | (aa >> (32 - 10)));

			// JJ(cc, dd, ee, aa, bb, X[15],  5);
			cc += (dd ^ (ee | ~aa)) + X[15] + 0xa953fd4e;
			cc = ((cc << 5) | (cc >> (32 - 5))) + bb;
			ee = ((ee << 10) | (ee >> (32 - 10)));

			// JJ(bb, cc, dd, ee, aa, X[13],  6);
			bb += (cc ^ (dd | ~ee)) + X[13] + 0xa953fd4e;
			bb = ((bb << 6) | (bb >> (32 - 6))) + aa;
			dd = ((dd << 10) | (dd >> (32 - 10)));

			// parallel round 1
			// JJJ(aaa, bbb, ccc, ddd, eee, X[ 5],  8);
			aaa += (bbb ^ (ccc | ~ddd)) + X[5] + 0x50a28be6;
			aaa = ((aaa << 8) | (aaa >> (32 - 8))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// JJJ(eee, aaa, bbb, ccc, ddd, X[14],  9);
			eee += (aaa ^ (bbb | ~ccc)) + X[14] + 0x50a28be6;
			eee = ((eee << 9) | (eee >> (32 - 9))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// JJJ(ddd, eee, aaa, bbb, ccc, X[ 7],  9);
			ddd += (eee ^ (aaa | ~bbb)) + X[7] + 0x50a28be6;
			ddd = ((ddd << 9) | (ddd >> (32 - 9))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// JJJ(ccc, ddd, eee, aaa, bbb, X[ 0], 11);
			ccc += (ddd ^ (eee | ~aaa)) + X[0] + 0x50a28be6;
			ccc = ((ccc << 11) | (ccc >> (32 - 11))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// JJJ(bbb, ccc, ddd, eee, aaa, X[ 9], 13);
			bbb += (ccc ^ (ddd | ~eee)) + X[9] + 0x50a28be6;
			bbb = ((bbb << 13) | (bbb >> (32 - 13))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// JJJ(aaa, bbb, ccc, ddd, eee, X[ 2], 15);
			aaa += (bbb ^ (ccc | ~ddd)) + X[2] + 0x50a28be6;
			aaa = ((aaa << 15) | (aaa >> (32 - 15))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// JJJ(eee, aaa, bbb, ccc, ddd, X[11], 15);
			eee += (aaa ^ (bbb | ~ccc)) + X[11] + 0x50a28be6;
			eee = ((eee << 15) | (eee >> (32 - 15))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// JJJ(ddd, eee, aaa, bbb, ccc, X[ 4],  5);
			ddd += (eee ^ (aaa | ~bbb)) + X[4] + 0x50a28be6;
			ddd = ((ddd << 5) | (ddd >> (32 - 5))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// JJJ(ccc, ddd, eee, aaa, bbb, X[13],  7);
			ccc += (ddd ^ (eee | ~aaa)) + X[13] + 0x50a28be6;
			ccc = ((ccc << 7) | (ccc >> (32 - 7))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// JJJ(bbb, ccc, ddd, eee, aaa, X[ 6],  7);
			bbb += (ccc ^ (ddd | ~eee)) + X[6] + 0x50a28be6;
			bbb = ((bbb << 7) | (bbb >> (32 - 7))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// JJJ(aaa, bbb, ccc, ddd, eee, X[15],  8);
			aaa += (bbb ^ (ccc | ~ddd)) + X[15] + 0x50a28be6;
			aaa = ((aaa << 8) | (aaa >> (32 - 8))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// JJJ(eee, aaa, bbb, ccc, ddd, X[ 8], 11);
			eee += (aaa ^ (bbb | ~ccc)) + X[8] + 0x50a28be6;
			eee = ((eee << 11) | (eee >> (32 - 11))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// JJJ(ddd, eee, aaa, bbb, ccc, X[ 1], 14);
			ddd += (eee ^ (aaa | ~bbb)) + X[1] + 0x50a28be6;
			ddd = ((ddd << 14) | (ddd >> (32 - 14))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// JJJ(ccc, ddd, eee, aaa, bbb, X[10], 14);
			ccc += (ddd ^ (eee | ~aaa)) + X[10] + 0x50a28be6;
			ccc = ((ccc << 14) | (ccc >> (32 - 14))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// JJJ(bbb, ccc, ddd, eee, aaa, X[ 3], 12);
			bbb += (ccc ^ (ddd | ~eee)) + X[3] + 0x50a28be6;
			bbb = ((bbb << 12) | (bbb >> (32 - 12))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// JJJ(aaa, bbb, ccc, ddd, eee, X[12],  6);
			aaa += (bbb ^ (ccc | ~ddd)) + X[12] + 0x50a28be6;
			aaa = ((aaa << 6) | (aaa >> (32 - 6))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// parallel round 2
			// III(eee, aaa, bbb, ccc, ddd, X[ 6],  9);
			eee += ((aaa & ccc) | (bbb & ~ccc)) + X[6] + 0x5c4dd124;
			eee = ((eee << 9) | (eee >> (32 - 9))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// III(ddd, eee, aaa, bbb, ccc, X[11], 13);
			ddd += ((eee & bbb) | (aaa & ~bbb)) + X[11] + 0x5c4dd124;
			ddd = ((ddd << 13) | (ddd >> (32 - 13))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// III(ccc, ddd, eee, aaa, bbb, X[ 3], 15);
			ccc += ((ddd & aaa) | (eee & ~aaa)) + X[3] + 0x5c4dd124;
			ccc = ((ccc << 15) | (ccc >> (32 - 15))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// III(bbb, ccc, ddd, eee, aaa, X[ 7],  7);
			bbb += ((ccc & eee) | (ddd & ~eee)) + X[7] + 0x5c4dd124;
			bbb = ((bbb << 7) | (bbb >> (32 - 7))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// III(aaa, bbb, ccc, ddd, eee, X[ 0], 12);
			aaa += ((bbb & ddd) | (ccc & ~ddd)) + X[0] + 0x5c4dd124;
			aaa = ((aaa << 12) | (aaa >> (32 - 12))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// III(eee, aaa, bbb, ccc, ddd, X[13],  8);
			eee += ((aaa & ccc) | (bbb & ~ccc)) + X[13] + 0x5c4dd124;
			eee = ((eee << 8) | (eee >> (32 - 8))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// III(ddd, eee, aaa, bbb, ccc, X[ 5],  9);
			ddd += ((eee & bbb) | (aaa & ~bbb)) + X[5] + 0x5c4dd124;
			ddd = ((ddd << 9) | (ddd >> (32 - 9))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// III(ccc, ddd, eee, aaa, bbb, X[10], 11);
			ccc += ((ddd & aaa) | (eee & ~aaa)) + X[10] + 0x5c4dd124;
			ccc = ((ccc << 11) | (ccc >> (32 - 11))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// III(bbb, ccc, ddd, eee, aaa, X[14],  7);
			bbb += ((ccc & eee) | (ddd & ~eee)) + X[14] + 0x5c4dd124;
			bbb = ((bbb << 7) | (bbb >> (32 - 7))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// III(aaa, bbb, ccc, ddd, eee, X[15],  7);
			aaa += ((bbb & ddd) | (ccc & ~ddd)) + X[15] + 0x5c4dd124;
			aaa = ((aaa << 7) | (aaa >> (32 - 7))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// III(eee, aaa, bbb, ccc, ddd, X[ 8], 12);
			eee += ((aaa & ccc) | (bbb & ~ccc)) + X[8] + 0x5c4dd124;
			eee = ((eee << 12) | (eee >> (32 - 12))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// III(ddd, eee, aaa, bbb, ccc, X[12],  7);
			ddd += ((eee & bbb) | (aaa & ~bbb)) + X[12] + 0x5c4dd124;
			ddd = ((ddd << 7) | (ddd >> (32 - 7))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// III(ccc, ddd, eee, aaa, bbb, X[ 4],  6);
			ccc += ((ddd & aaa) | (eee & ~aaa)) + X[4] + 0x5c4dd124;
			ccc = ((ccc << 6) | (ccc >> (32 - 6))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// III(bbb, ccc, ddd, eee, aaa, X[ 9], 15);
			bbb += ((ccc & eee) | (ddd & ~eee)) + X[9] + 0x5c4dd124;
			bbb = ((bbb << 15) | (bbb >> (32 - 15))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// III(aaa, bbb, ccc, ddd, eee, X[ 1], 13);
			aaa += ((bbb & ddd) | (ccc & ~ddd)) + X[1] + 0x5c4dd124;
			aaa = ((aaa << 13) | (aaa >> (32 - 13))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// III(eee, aaa, bbb, ccc, ddd, X[ 2], 11);
			eee += ((aaa & ccc) | (bbb & ~ccc)) + X[2] + 0x5c4dd124;
			eee = ((eee << 11) | (eee >> (32 - 11))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// parallel round 3
			// HHH(ddd, eee, aaa, bbb, ccc, X[15],  9);
			ddd += ((eee | ~aaa) ^ bbb) + X[15] + 0x6d703ef3;
			ddd = ((ddd << 9) | (ddd >> (32 - 9))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// HHH(ccc, ddd, eee, aaa, bbb, X[ 5],  7);
			ccc += ((ddd | ~eee) ^ aaa) + X[5] + 0x6d703ef3;
			ccc = ((ccc << 7) | (ccc >> (32 - 7))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// HHH(bbb, ccc, ddd, eee, aaa, X[ 1], 15);
			bbb += ((ccc | ~ddd) ^ eee) + X[1] + 0x6d703ef3;
			bbb = ((bbb << 15) | (bbb >> (32 - 15))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// HHH(aaa, bbb, ccc, ddd, eee, X[ 3], 11);
			aaa += ((bbb | ~ccc) ^ ddd) + X[3] + 0x6d703ef3;
			aaa = ((aaa << 11) | (aaa >> (32 - 11))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// HHH(eee, aaa, bbb, ccc, ddd, X[ 7],  8);
			eee += ((aaa | ~bbb) ^ ccc) + X[7] + 0x6d703ef3;
			eee = ((eee << 8) | (eee >> (32 - 8))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// HHH(ddd, eee, aaa, bbb, ccc, X[14],  6);
			ddd += ((eee | ~aaa) ^ bbb) + X[14] + 0x6d703ef3;
			ddd = ((ddd << 6) | (ddd >> (32 - 6))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// HHH(ccc, ddd, eee, aaa, bbb, X[ 6],  6);
			ccc += ((ddd | ~eee) ^ aaa) + X[6] + 0x6d703ef3;
			ccc = ((ccc << 6) | (ccc >> (32 - 6))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// HHH(bbb, ccc, ddd, eee, aaa, X[ 9], 14);
			bbb += ((ccc | ~ddd) ^ eee) + X[9] + 0x6d703ef3;
			bbb = ((bbb << 14) | (bbb >> (32 - 14))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// HHH(aaa, bbb, ccc, ddd, eee, X[11], 12);
			aaa += ((bbb | ~ccc) ^ ddd) + X[11] + 0x6d703ef3;
			aaa = ((aaa << 12) | (aaa >> (32 - 12))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// HHH(eee, aaa, bbb, ccc, ddd, X[ 8], 13);
			eee += ((aaa | ~bbb) ^ ccc) + X[8] + 0x6d703ef3;
			eee = ((eee << 13) | (eee >> (32 - 13))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// HHH(ddd, eee, aaa, bbb, ccc, X[12],  5);
			ddd += ((eee | ~aaa) ^ bbb) + X[12] + 0x6d703ef3;
			ddd = ((ddd << 5) | (ddd >> (32 - 5))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// HHH(ccc, ddd, eee, aaa, bbb, X[ 2], 14);
			ccc += ((ddd | ~eee) ^ aaa) + X[2] + 0x6d703ef3;
			ccc = ((ccc << 14) | (ccc >> (32 - 14))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// HHH(bbb, ccc, ddd, eee, aaa, X[10], 13);
			bbb += ((ccc | ~ddd) ^ eee) + X[10] + 0x6d703ef3;
			bbb = ((bbb << 13) | (bbb >> (32 - 13))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// HHH(aaa, bbb, ccc, ddd, eee, X[ 0], 13);
			aaa += ((bbb | ~ccc) ^ ddd) + X[0] + 0x6d703ef3;
			aaa = ((aaa << 13) | (aaa >> (32 - 13))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// HHH(eee, aaa, bbb, ccc, ddd, X[ 4],  7);
			eee += ((aaa | ~bbb) ^ ccc) + X[4] + 0x6d703ef3;
			eee = ((eee << 7) | (eee >> (32 - 7))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// HHH(ddd, eee, aaa, bbb, ccc, X[13],  5);
			ddd += ((eee | ~aaa) ^ bbb) + X[13] + 0x6d703ef3;
			ddd = ((ddd << 5) | (ddd >> (32 - 5))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// parallel round 4
			// GGG(ccc, ddd, eee, aaa, bbb, X[ 8], 15);
			ccc += ((ddd & eee) | (~ddd & aaa)) + X[8] + 0x7a6d76e9;
			ccc = ((ccc << 15) | (ccc >> (32 - 15))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// GGG(bbb, ccc, ddd, eee, aaa, X[ 6],  5);
			bbb += ((ccc & ddd) | (~ccc & eee)) + X[6] + 0x7a6d76e9;
			bbb = ((bbb << 5) | (bbb >> (32 - 5))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// GGG(aaa, bbb, ccc, ddd, eee, X[ 4],  8);
			aaa += ((bbb & ccc) | (~bbb & ddd)) + X[4] + 0x7a6d76e9;
			aaa = ((aaa << 8) | (aaa >> (32 - 8))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// GGG(eee, aaa, bbb, ccc, ddd, X[ 1], 11);
			eee += ((aaa & bbb) | (~aaa & ccc)) + X[1] + 0x7a6d76e9;
			eee = ((eee << 11) | (eee >> (32 - 11))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// GGG(ddd, eee, aaa, bbb, ccc, X[ 3], 14);
			ddd += ((eee & aaa) | (~eee & bbb)) + X[3] + 0x7a6d76e9;
			ddd = ((ddd << 14) | (ddd >> (32 - 14))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// GGG(ccc, ddd, eee, aaa, bbb, X[11], 14);
			ccc += ((ddd & eee) | (~ddd & aaa)) + X[11] + 0x7a6d76e9;
			ccc = ((ccc << 14) | (ccc >> (32 - 14))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// GGG(bbb, ccc, ddd, eee, aaa, X[15],  6);
			bbb += ((ccc & ddd) | (~ccc & eee)) + X[15] + 0x7a6d76e9;
			bbb = ((bbb << 6) | (bbb >> (32 - 6))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// GGG(aaa, bbb, ccc, ddd, eee, X[ 0], 14);
			aaa += ((bbb & ccc) | (~bbb & ddd)) + X[0] + 0x7a6d76e9;
			aaa = ((aaa << 14) | (aaa >> (32 - 14))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// GGG(eee, aaa, bbb, ccc, ddd, X[ 5],  6);
			eee += ((aaa & bbb) | (~aaa & ccc)) + X[5] + 0x7a6d76e9;
			eee = ((eee << 6) | (eee >> (32 - 6))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// GGG(ddd, eee, aaa, bbb, ccc, X[12],  9);
			ddd += ((eee & aaa) | (~eee & bbb)) + X[12] + 0x7a6d76e9;
			ddd = ((ddd << 9) | (ddd >> (32 - 9))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// GGG(ccc, ddd, eee, aaa, bbb, X[ 2], 12);
			ccc += ((ddd & eee) | (~ddd & aaa)) + X[2] + 0x7a6d76e9;
			ccc = ((ccc << 12) | (ccc >> (32 - 12))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// GGG(bbb, ccc, ddd, eee, aaa, X[13],  9);
			bbb += ((ccc & ddd) | (~ccc & eee)) + X[13] + 0x7a6d76e9;
			bbb = ((bbb << 9) | (bbb >> (32 - 9))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// GGG(aaa, bbb, ccc, ddd, eee, X[ 9], 12);
			aaa += ((bbb & ccc) | (~bbb & ddd)) + X[9] + 0x7a6d76e9;
			aaa = ((aaa << 12) | (aaa >> (32 - 12))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// GGG(eee, aaa, bbb, ccc, ddd, X[ 7],  5);
			eee += ((aaa & bbb) | (~aaa & ccc)) + X[7] + 0x7a6d76e9;
			eee = ((eee << 5) | (eee >> (32 - 5))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// GGG(ddd, eee, aaa, bbb, ccc, X[10], 15);
			ddd += ((eee & aaa) | (~eee & bbb)) + X[10] + 0x7a6d76e9;
			ddd = ((ddd << 15) | (ddd >> (32 - 15))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// GGG(ccc, ddd, eee, aaa, bbb, X[14],  8);
			ccc += ((ddd & eee) | (~ddd & aaa)) + X[14] + 0x7a6d76e9;
			ccc = ((ccc << 8) | (ccc >> (32 - 8))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// parallel round 5
			// FFF(bbb, ccc, ddd, eee, aaa, X[12] ,  8);
			bbb += (ccc ^ ddd ^ eee) + X[12];
			bbb = ((bbb << 8) | (bbb >> (32 - 8))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// FFF(aaa, bbb, ccc, ddd, eee, X[15] ,  5);
			aaa += (bbb ^ ccc ^ ddd) + X[15];
			aaa = ((aaa << 5) | (aaa >> (32 - 5))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// FFF(eee, aaa, bbb, ccc, ddd, X[10] , 12);
			eee += (aaa ^ bbb ^ ccc) + X[10];
			eee = ((eee << 12) | (eee >> (32 - 12))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// FFF(ddd, eee, aaa, bbb, ccc, X[ 4] ,  9);
			ddd += (eee ^ aaa ^ bbb) + X[4];
			ddd = ((ddd << 9) | (ddd >> (32 - 9))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// FFF(ccc, ddd, eee, aaa, bbb, X[ 1] , 12);
			ccc += (ddd ^ eee ^ aaa) + X[1];
			ccc = ((ccc << 12) | (ccc >> (32 - 12))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// FFF(bbb, ccc, ddd, eee, aaa, X[ 5] ,  5);
			bbb += (ccc ^ ddd ^ eee) + X[5];
			bbb = ((bbb << 5) | (bbb >> (32 - 5))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// FFF(aaa, bbb, ccc, ddd, eee, X[ 8] , 14);
			aaa += (bbb ^ ccc ^ ddd) + X[8];
			aaa = ((aaa << 14) | (aaa >> (32 - 14))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// FFF(eee, aaa, bbb, ccc, ddd, X[ 7] ,  6);
			eee += (aaa ^ bbb ^ ccc) + X[7];
			eee = ((eee << 6) | (eee >> (32 - 6))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// FFF(ddd, eee, aaa, bbb, ccc, X[ 6] ,  8);
			ddd += (eee ^ aaa ^ bbb) + X[6];
			ddd = ((ddd << 8) | (ddd >> (32 - 8))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// FFF(ccc, ddd, eee, aaa, bbb, X[ 2] , 13);
			ccc += (ddd ^ eee ^ aaa) + X[2];
			ccc = ((ccc << 13) | (ccc >> (32 - 13))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// FFF(bbb, ccc, ddd, eee, aaa, X[13] ,  6);
			bbb += (ccc ^ ddd ^ eee) + X[13];
			bbb = ((bbb << 6) | (bbb >> (32 - 6))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// FFF(aaa, bbb, ccc, ddd, eee, X[14] ,  5);
			aaa += (bbb ^ ccc ^ ddd) + X[14];
			aaa = ((aaa << 5) | (aaa >> (32 - 5))) + eee;
			ccc = ((ccc << 10) | (ccc >> (32 - 10)));

			// FFF(eee, aaa, bbb, ccc, ddd, X[ 0] , 15);
			eee += (aaa ^ bbb ^ ccc) + X[0];
			eee = ((eee << 15) | (eee >> (32 - 15))) + ddd;
			bbb = ((bbb << 10) | (bbb >> (32 - 10)));

			// FFF(ddd, eee, aaa, bbb, ccc, X[ 3] , 13);
			ddd += (eee ^ aaa ^ bbb) + X[3];
			ddd = ((ddd << 13) | (ddd >> (32 - 13))) + ccc;
			aaa = ((aaa << 10) | (aaa >> (32 - 10)));

			// FFF(ccc, ddd, eee, aaa, bbb, X[ 9] , 11);
			ccc += (ddd ^ eee ^ aaa) + X[9];
			ccc = ((ccc << 11) | (ccc >> (32 - 11))) + bbb;
			eee = ((eee << 10) | (eee >> (32 - 10)));

			// FFF(bbb, ccc, ddd, eee, aaa, X[11] , 11);
			bbb += (ccc ^ ddd ^ eee) + X[11];
			bbb = ((bbb << 11) | (bbb >> (32 - 11))) + aaa;
			ddd = ((ddd << 10) | (ddd >> (32 - 10)));

			// Combine results.
			ddd += cc + H1;
			H1 = H2 + dd + eee;
			H2 = H3 + ee + aaa;
			H3 = H4 + aa + bbb;
			H4 = H0 + bb + ccc;
			H0 = ddd;

			// Reset the offset and clean out the word buffer.
			xOff = 0;
			Array.Clear(X, 0, X.Length);
		}
	}
}
