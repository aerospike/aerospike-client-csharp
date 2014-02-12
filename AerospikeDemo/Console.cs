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
using System.Text;
using System.Windows.Forms;
using Aerospike.Client;

namespace Aerospike.Demo
{
	/// <summary>
	/// Log base class.  To create a new logging target, inherit from this class and override Write().
	/// </summary>
	public abstract class Console
	{
		/// <summary>
		/// Log info message.
		/// </summary>
		public void Info(string format, params object[] args)
		{
			Write(Log.Level.INFO, format, args);
		}

		/// <summary>
		/// Log warning message.
		/// </summary>
		public void Warn(string format, params object[] args)
		{
			Write(Log.Level.WARN, format, args);
		}

		/// <summary>
		/// Log info message.
		/// </summary>
		public void Error(string format, params object[] args)
		{
			Write(Log.Level.ERROR, format, args);
		}

		/// <summary>
		/// Log message.
		/// </summary>
		public void Write(Log.Level level, string format, params object[] args)
		{
			string dt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
			string message;

			if (args.Length > 0)
			{
				message = string.Format(format, args);
			}
			else
			{
				message = format;
			}
			Write(dt + ' ' + level.ToString() + ' ' + message);
		}

		/// <summary>
		/// Log line without timestamp or level.
		/// </summary>
		public void Write(string format, params object[] args)
		{
			Write(string.Format(format, args));
		}

		/// <summary>
		/// Log line without timestamp or level.
		/// </summary>
		public void Write(string message)
		{
			WriteBox(message + System.Environment.NewLine);
		}

		public abstract void WriteBox(string message);
		public abstract void Clear();
	}
	
	/// <summary>
    /// Console logger with RichTextBox GUI target.
    /// </summary>
    public class ConsoleBox : Console
    {
		private delegate void WriteLogDelegate(string message);
		private delegate void ClearLogDelegate();
		private TextBox box;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="box">Target GUI control.</param>
        public ConsoleBox(TextBox box)
        {
            this.box = box;
        }

        public override void WriteBox(string message)
        {
			box.Invoke(new WriteLogDelegate(WriteLog), message);
        }

        private void WriteLog(string message)
        {
			box.AppendText(message);
        }

        public override void Clear()
        {
			box.Invoke(new ClearLogDelegate(ClearLog));
        }

        private void ClearLog()
        {
            box.Clear();
            box.Refresh();
        }
    }
}
