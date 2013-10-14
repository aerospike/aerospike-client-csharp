using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Aerospike.Demo
{
	public abstract class Example
	{
		protected internal Console console;
		protected volatile bool valid;

		public Example(Console console)
		{
			this.console = console;
		}

		public void Stop()
		{
			valid = false;
		}

		public void Run(Arguments args)
		{
			valid = true;
			console.Clear();
			console.Info(this.GetType().Name + " Begin");
			RunExample(args);
			console.Info(this.GetType().Name + " End");
		}

		public abstract void RunExample(Arguments args);
	}
}
