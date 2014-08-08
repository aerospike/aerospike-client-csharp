/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
