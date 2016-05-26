/*
 * Copyright (c) 2015, OpenSlate <mike.omalley@openslatedata.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

//package org.apache.cassandra.io.sstable

@Grab('org.apache.cassandra:cassandra-all:3.0.6')
@Grab('com.xlson.groovycsv:groovycsv:1.1')
@Grab('com.opencsv:opencsv:3.4')
@Grab('com.datastax.cassandra:cassandra-driver-core:3.0.0')


import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static com.xlson.groovycsv.CsvParser.parseCsv

import groovy.json.JsonSlurper
import com.opencsv.CSVReader
import java.text.SimpleDateFormat

import org.apache.cassandra.config.Config
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.cassandra.exceptions.InvalidRequestException
import org.apache.cassandra.io.sstable.CQLSSTableWriter

// imports for subclass of CQLSSTableWriter
import org.apache.cassandra.cql3.*
import org.apache.cassandra.cql3.functions.*
import org.apache.cassandra.utils.ByteBufferUtil
import com.datastax.driver.core.ProtocolVersion
import org.apache.cassandra.config.Schema
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.utils.Pair
import org.apache.cassandra.io.sstable.AbstractSSTableSimpleWriter
import org.apache.cassandra.io.sstable.SSTableSimpleWriter
import org.apache.cassandra.io.sstable.format.SSTableFormat
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter
import org.apache.cassandra.cql3.statements.*
import org.apache.cassandra.schema.*
import org.apache.cassandra.exceptions.*
import org.apache.cassandra.dht.*
import org.apache.cassandra.service.*
import java.nio.ByteBuffer


DATE_FORMAT = null
FILTERS = [:]
DECIMAL_PATTERN = ~/\.0$/


class Things {
	def static parseStatement(String query, String type)
	{
		try
		{
			ParsedStatement stmt = QueryProcessor.parseStatement(query);
	
			return UpdateStatement.ParsedInsert.class.cast(stmt);
		}
		catch (RequestValidationException e)
		{
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}
	
	static List<ColumnSpecification> prepareInsert(String insert_string)
	{
		ParsedStatement.Prepared cqlStatement = parseStatement(insert_string, "INSERT").prepare()
		UpdateStatement insert = (UpdateStatement) cqlStatement.statement;
		insert.validate(ClientState.forInternalCalls());
	
		if (insert.hasConditions())
			throw new IllegalArgumentException("Conditional statements are not supported");
		if (insert.isCounter())
			throw new IllegalArgumentException("Counter update statements are not supported");
		if (cqlStatement.boundNames.isEmpty())
			throw new IllegalArgumentException("Provided insert statement has no bind variables");
	
		return cqlStatement.boundNames;
	}
}
	
class NulllessWriterProxy  extends groovy.util.Proxy {
	def addRow(String insert, Map<String, Object> values)
	{
		List<ColumnSpecification> boundNames = Things.prepareInsert(insert)
		List<TypeCodec> typeCodecs;
		int size = boundNames.size();
		List<ByteBuffer> rawValues = new ArrayList<>(size);
		typeCodecs = boundNames.stream().map(bn ->  UDHelper.codecFor(UDHelper.driverType(bn.type)))
                                             .collect(Collectors.toList());
		for (int i = 0; i < size; i++)
		{
			ColumnSpecification spec = boundNames.get(i);
			Object value = values.get(spec.name.toString());

			rawValues.add(value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : typeCodecs.get(i).serialize(value, ProtocolVersion.NEWEST_SUPPORTED));
		}
		return rawAddRow(rawValues);
	}
}

//class NoNullSSTableWriter extends CQLSSTableWriter 
//{
//	//private final AbstractSSTableSimpleWriter writer;
//
//	public NoNullSSTableWriter addRow(Map<String, Object> values)
//	throws InvalidRequestException, IOException
//	{
//		int size = boundNames.size();
//		List<ByteBuffer> rawValues = new ArrayList<>(size);
//		for (int i = 0; i < size; i++)
//		{
//			ColumnSpecification spec = boundNames.get(i);
//			Object value = values.get(spec.name.toString());
//
//			rawValues.add(value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : typeCodecs.get(i).serialize(value, ProtocolVersion.NEWEST_SUPPORTED));
//		}
//		return rawAddRow(rawValues);
//	}
//
//	public static MyBuilder builder()
//	{
//		println "Instantiating MyBuilder()"
//		return new MyBuilder();
//	}
//
//	public static class MyBuilder extends CQLSSTableWriter.Builder
//	{
//
//		private File directory;
//
//		protected SSTableFormat.Type formatType = null;
//
//		private CreateTableStatement.RawStatement schemaStatement;
//		private final List<CreateTypeStatement> typeStatements;
//		private UpdateStatement.ParsedInsert insertStatement;
//		private IPartitioner partitioner;
//
//		private boolean sorted = false;
//		private long bufferSizeInMB = 128;
//
//		public NoNullSSTableWriter build()
//		{
//			println "Calling Subclassed build()";
//
//			if (directory == null)
//				throw new IllegalStateException("No ouptut directory specified, you should provide a directory with inDirectory()");
//			if (schemaStatement == null)
//				throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");
//			if (insertStatement == null)
//				throw new IllegalStateException("No insert statement specified, you should provide an insert statement through using()");
//
//			synchronized (NoNullSSTableWriter.class)
//			{
//				String keyspace = schemaStatement.keyspace();
//
//				if (Schema.instance.getKSMetaData(keyspace) == null)
//					Schema.instance.load(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)));
//
//				createTypes(keyspace);
//				CFMetaData cfMetaData = createTable(keyspace);
//				Pair<UpdateStatement, List<ColumnSpecification>> preparedInsert = prepareInsert();
//
//				/*
//				AbstractSSTableSimpleWriter mywriter = sorted
//													 ? new SSTableSimpleWriter(directory, cfMetaData, preparedInsert.left.updatedColumns())
//													 : new SSTableSimpleUnsortedWriter(directory, cfMetaData, preparedInsert.left.updatedColumns(), bufferSizeInMB);
//				*/
//				Object mywriter;
//				if (sorted)
//					mywriter = new SSTableSimpleWriter(directory, cfMetaData, preparedInsert.left.updatedColumns())
//				else
//					mywriter = new SSTableSimpleUnsortedWriter(directory, cfMetaData, preparedInsert.left.updatedColumns(), bufferSizeInMB);
//
//				if (formatType != null)
//					mywriter.setSSTableFormatType(formatType);
//
//				return new NoNullSSTableWriter((AbstractSSTableSimpleWriter)mywriter, preparedInsert.left, preparedInsert.right);
//			}
//		}
//
//		@SuppressWarnings("resource")
//		protected MyBuilder() {
//			this.typeStatements = new ArrayList<>();
//		}
//
//		public MyBuilder inDirectory(String directory)
//		{
//			return inDirectory(new File(directory));
//		}
//
//		public MyBuilder inDirectory(File directory)
//		{
//			if (!directory.exists())
//				throw new IllegalArgumentException(directory + " doesn't exists");
//			if (!directory.canWrite())
//				throw new IllegalArgumentException(directory + " exists but is not writable");
//
//			this.directory = directory;
//			return this;
//		}
//
//		public MyBuilder withType(String typeDefinition) throws SyntaxException
//		{
//			typeStatements.add(parseStatement(typeDefinition, CreateTypeStatement.class, "CREATE TYPE"));
//			return this;
//		}
//
//		public MyBuilder forTable(String schema)
//		{
//			this.schemaStatement = parseStatement(schema, CreateTableStatement.RawStatement.class, "CREATE TABLE");
//			return this;
//		}
//
//		public MyBuilder withPartitioner(IPartitioner partitioner)
//		{
//			this.partitioner = partitioner;
//			return this;
//		}
//
//		public MyBuilder using(String insert)
//		{
//			this.insertStatement = parseStatement(insert, UpdateStatement.ParsedInsert.class, "INSERT");
//			return this;
//		}
//
//		public MyBuilder withBufferSizeInMB(int size)
//		{
//			this.bufferSizeInMB = size;
//			return this;
//		}
//
//		public MyBuilder sorted()
//		{
//			this.sorted = true;
//			return this;
//		}
//
//		private void createTypes(String keyspace)
//		{
//			KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
//			Types.RawBuilder builder = Types.rawBuilder(keyspace);
//			for (CreateTypeStatement st : typeStatements)
//				st.addToRawBuilder(builder);
//
//			ksm = ksm.withSwapped(builder.build());
//			Schema.instance.setKeyspaceMetadata(ksm);
//		}
//
//		private CFMetaData createTable(String keyspace)
//		{
//			KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
//
//			CFMetaData cfMetaData = ksm.tables.getNullable(schemaStatement.columnFamily());
//			if (cfMetaData == null)
//			{
//				CreateTableStatement statement = (CreateTableStatement) schemaStatement.prepare(ksm.types).statement;
//				statement.validate(ClientState.forInternalCalls());
//
//				cfMetaData = statement.getCFMetaData();
//
//				Schema.instance.load(cfMetaData);
//				Schema.instance.setKeyspaceMetadata(ksm.withSwapped(ksm.tables.with(cfMetaData)));
//			}
//
//			if (partitioner != null)
//				return cfMetaData.copy(partitioner);
//			else
//				return cfMetaData;
//		}
//
//		private Pair<UpdateStatement, List<ColumnSpecification>> prepareInsert()
//		{
//			ParsedStatement.Prepared cqlStatement = insertStatement.prepare();
//			UpdateStatement insert = (UpdateStatement) cqlStatement.statement;
//			insert.validate(ClientState.forInternalCalls());
//
//			if (insert.hasConditions())
//				throw new IllegalArgumentException("Conditional statements are not supported");
//			if (insert.isCounter())
//				throw new IllegalArgumentException("Counter update statements are not supported");
//			if (cqlStatement.boundNames.isEmpty())
//				throw new IllegalArgumentException("Provided insert statement has no bind variables");
//
//			return Pair.create(insert, cqlStatement.boundNames);
//		}
//	}
//	private static <T extends ParsedStatement> T parseStatement(String query, Class<T> klass, String type)
//	{
//		try
//		{
//			ParsedStatement stmt = QueryProcessor.parseStatement(query);
//
//			if (!stmt.getClass().equals(klass))
//				throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());
//
//			return klass.cast(stmt);
//		}
//		catch (RequestValidationException e)
//		{
//			throw new IllegalArgumentException(e.getMessage(), e);
//		}
//	}
//}

def load_config(filename)
{
	return new JsonSlurper().parseText(new File(filename).text)
}

def build_insert(config)
{
	def stmnt = "INSERT INTO $config.keyspace.$config.table ("
	def fieldNames = []
	def placeholders = []
	config.fields.each {
		fieldNames.add(it.name)
		placeholders.add("?")
	}
	stmnt += fieldNames.join(", ") + ") VALUES (" + placeholders.join(", ") + ")"
	return stmnt
}

def build_schema(config)
{
	def s = "CREATE TABLE $config.keyspace.$config.table ("
	def fields = []
	def clustering = config.clustering ? "WITH CLUSTERING ORDER BY ($config.clustering)" : ""
	config.fields.each {
		fields.add("$it.name $it.type")
	}
	return s + fields.join(",\n") + ",\nprimary key " + config.primary_key + ") ${clustering}"
}

def process_field(name, type, value, line, filter)
{
	if (!value || value == "NaN" || value == "Infinity") type = null
	switch (type) {
		case null:
			value = null
			break
		case "int":
			value = value.replaceAll(DECIMAL_PATTERN, '')
			value = value.toInteger()
			break
		case "bigint":
			value = Long.parseLong(value)
			break
		case "decimal":
			value = value.replaceAll(DECIMAL_PATTERN, '')
			value = new BigDecimal(value)
			break
		case "timestamp":
			value = DATE_FORMAT.parse(value)
			break
		case "boolean":
			value = Boolean.valueOf(value)
			break
			
	}
	if (filter != null) {
		def f
		if (FILTERS[name]) {
			f = FILTERS[name]
		} else {
			if (config.filter_imports) {
				with_imports = config.filter_imports.collect { "import " + it }.join(";") + "; " + filter
				println(with_imports)
				f = evaluate(with_imports)
			} else {
				f = evaluate(filter)
			}
			f.delegate = this
			FILTERS[name] = f
		}
		value = f(value, line)
	}
	return value
}

def F(name, value, line)
{
	def f = FILTERS[name]
	return f(value, line)
}

def parse_json(string)
{
	try {
		return new JsonSlurper().parseText(string)
	} catch (e) {
		println string;
		throw e;
	}
}

def parse_csv(string)
{
	return new CSVReader(new StringReader(string));
}

def make_row(config, line)
{
	def row = [:]
	config.fields.each {
		try {
			row[it.name] = process_field(it.name, it.type, line[it.name], line, config.filters.get(it.name, null))
		} catch (Exception e) {
			println "ERROR process_field: name :: ${it.name} :: type ${it.type}"
			throw e
		}
	}
	return row
}

def main(String[] args)
{
	set_log_level()
	cli = new CliBuilder(usage: 'load.groovy -d csvfile -c configfile [-o outputdirectory] [-h] [-v]')
	cli.with {
		h longOpt: 'help', 'Show usage information'
		c longOpt: 'config', 'Config File', required: true, args:1
		d longOpt: 'data', 'Data File', required: true, args:1
		o longOpt: 'outputdir', 'Output Directory', args:1
		s longOpt: 'printschema', 'Just print the schema'
	}

	def options = cli.parse(args)
	if (!options || options.h) {
		return
	}

	config = load_config(options.c)
	DATE_FORMAT = new SimpleDateFormat(config.date_format)
	def insert_statement = build_insert(config)
	def schema = build_schema(config)

	if (options.s) {
		println schema + ';'
		return
	}

	// magic!
	Config.setClientMode(true)

	// Create output directory that has keyspace and table name in the path
	def prefix
	if (options.o) prefix = options.o
	else prefix = './data'
	File outputDir = new File(prefix + "/$config.keyspace/$config.table")
	if (!outputDir.exists() && !outputDir.mkdirs())
	{
		throw new RuntimeException("Cannot create output directory: " + outputDir)
	}

	def builder = CQLSSTableWriter.builder()
//	def builder = NoNullSSTableWriter.builder()
	builder.inDirectory(outputDir)
			.forTable(schema)
			.using(insert_statement)
			.withPartitioner(new Murmur3Partitioner())
	def writer = new NulllessWriterProxy().wrap(builder.build())

	String filename = options.d

	def headers = config.fields.collect { it.name }

	def data = parseCsv(
				new BufferedReader(
					new InputStreamReader(
						new FileInputStream(filename),
						"UTF-8")
					),
				readFirstLine: true,
				columnNames: headers)

	int c = 0
	try {
		for(line in data) {
			if (++c % 1000 == 0 || ! data.hasNext()) println c
			def row = make_row(config, line)
			writer.addRow(insert_statement, row)
		}
	} catch (Exception e) {
		println e
		println "Exception caught at data line: ${c}"
		org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e).printStackTrace()
	}
	writer.close()
}

def set_log_level() {
	Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
	root.setLevel(Level.WARN);
}

main(args)
