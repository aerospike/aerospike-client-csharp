using System.Text;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace Aerospike.Compatibility.Generator
{
	[Generator]
	public sealed class LegacyInterfaceGenerator : ISourceGenerator
	{
		private static readonly SymbolDisplayFormat TypeFormat =
			SymbolDisplayFormat.FullyQualifiedFormat
				.WithMiscellaneousOptions(
					SymbolDisplayMiscellaneousOptions
						.EscapeKeywordIdentifiers);

		public void Initialize(GeneratorInitializationContext context)
		{
		}

		public void Execute(GeneratorExecutionContext context)
		{
			INamedTypeSymbol contract =
				context.Compilation.GetTypeByMetadataName(
					"Aerospike.Client.IAerospikeClient");

			if (contract == null)
			{
				return;
			}

			StringBuilder source = new();
			source.AppendLine("namespace Aerospike.Compatibility.Released850");
			source.AppendLine("{");
			source.AppendLine(
				"\tpublic sealed class LegacyClient : global::Aerospike.Client.IAerospikeClient");
			source.AppendLine("\t{");

			foreach (INamedTypeSymbol interfaceType in
				contract.AllInterfaces.Reverse().Append(contract))
			{
				foreach (ISymbol member in interfaceType.GetMembers())
				{
					switch (member)
					{
						case IMethodSymbol method
							when method.MethodKind ==
								MethodKind.Ordinary:
							AppendMethod(source, interfaceType, method);
							break;

						case IPropertySymbol property:
							AppendProperty(
								source,
								interfaceType,
								property);
							break;

						case IEventSymbol eventSymbol:
							AppendEvent(
								source,
								interfaceType,
								eventSymbol);
							break;
					}
				}
			}

			source.AppendLine("\t}");
			source.AppendLine("}");
			context.AddSource(
				"LegacyClient.g.cs",
				SourceText.From(source.ToString(), Encoding.UTF8));
		}

		private static void AppendMethod(
			StringBuilder source,
			INamedTypeSymbol interfaceType,
			IMethodSymbol method)
		{
			string returnType =
				method.ReturnType.ToDisplayString(TypeFormat);
			if (returnType.Contains("System.Void"))
			{
				returnType = "void";
			}
			source.Append("\t\t");
			AppendRefKind(source, method.RefKind);
			source.Append(returnType);
			source.Append(' ');
			source.Append(interfaceType.ToDisplayString(TypeFormat));
			source.Append('.');
			source.Append(Escape(method.Name));

			if (method.TypeParameters.Length > 0)
			{
				source.Append('<');
				source.Append(string.Join(
					", ",
					method.TypeParameters.Select(
						parameter => Escape(parameter.Name))));
				source.Append('>');
			}

			source.Append('(');
			AppendParameters(source, method.Parameters);
			source.AppendLine(")");
			source.AppendLine("\t\t{");

			source.AppendLine(
				"\t\t\tthrow new global::System.NotSupportedException();");
			source.AppendLine("\t\t}");
		}

		private static void AppendProperty(
			StringBuilder source,
			INamedTypeSymbol interfaceType,
			IPropertySymbol property)
		{
			source.Append("\t\t");
			AppendRefKind(source, property.RefKind);
			source.Append(property.Type.ToDisplayString(TypeFormat));
			source.Append(' ');
			source.Append(interfaceType.ToDisplayString(TypeFormat));
			source.Append('.');

			if (property.IsIndexer)
			{
				source.Append("this[");
				AppendParameters(source, property.Parameters);
				source.Append(']');
			}
			else
			{
				source.Append(Escape(property.Name));
			}
			source.AppendLine();
			source.AppendLine("\t\t{");
			if (property.GetMethod != null)
			{
				source.AppendLine("\t\t\tget => default;");
			}
			if (property.SetMethod != null)
			{
				source.AppendLine("\t\t\tset { }");
			}
			source.AppendLine("\t\t}");
		}

		private static void AppendEvent(
			StringBuilder source,
			INamedTypeSymbol interfaceType,
			IEventSymbol eventSymbol)
		{
			source.Append("\t\tevent ");
			source.Append(
				eventSymbol.Type.ToDisplayString(TypeFormat));
			source.Append(' ');
			source.Append(interfaceType.ToDisplayString(TypeFormat));
			source.Append('.');
			source.Append(Escape(eventSymbol.Name));
			source.AppendLine();
			source.AppendLine("\t\t{");
			source.AppendLine("\t\t\tadd { }");
			source.AppendLine("\t\t\tremove { }");
			source.AppendLine("\t\t}");
		}

		private static void AppendParameters(
			StringBuilder source,
			ImmutableArray<IParameterSymbol> parameters)
		{
			for (int i = 0; i < parameters.Length; i++)
			{
				if (i > 0)
				{
					source.Append(", ");
				}
				IParameterSymbol parameter = parameters[i];
				AppendRefKind(source, parameter.RefKind);
				source.Append(
					parameter.Type.ToDisplayString(TypeFormat));
				source.Append(' ');
				source.Append(Escape(parameter.Name));
			}
		}

		private static void AppendRefKind(
			StringBuilder source,
			RefKind refKind)
		{
			switch (refKind)
			{
				case RefKind.Ref:
					source.Append("ref ");
					break;
				case RefKind.Out:
					source.Append("out ");
					break;
				case RefKind.In:
					source.Append("in ");
					break;
			}
		}

		private static string Escape(string identifier)
		{
			return SyntaxFacts.GetKeywordKind(identifier) ==
				SyntaxKind.None
					? identifier
					: "@" + identifier;
		}
	}
}
