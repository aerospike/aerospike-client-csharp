﻿// 
// Copyright (c) 2006 Damien Miller <djm@mindrot.org>
// Copyright (c) 2013 Ryan D. Emerle
// 
// Permission to use, copy, modify, and distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

namespace Aerospike.Client
{
	/// <summary>Exception for signalling parse errors. </summary>
	public class SaltParseException : Exception
	{
		/// <summary>Default constructor. </summary>
		public SaltParseException()
		{
		}

		/// <summary>Initializes a new instance of <see cref="SaltParseException"/>.</summary>
		/// <param name="message">The message.</param>
		public SaltParseException(string message)
			: base(message)
		{
		}

		/// <summary>Initializes a new instance of <see cref="SaltParseException"/>.</summary>
		/// <param name="message">       The message.</param>
		/// <param name="innerException">The inner exception.</param>
		public SaltParseException(string message, Exception innerException)
			: base(message, innerException)
		{
		}
	}
}
