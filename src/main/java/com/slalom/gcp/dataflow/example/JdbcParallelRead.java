package com.slalom.gcp.dataflow.example;

import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;
import com.mchange.v2.c3p0.ComboPooledDataSource;


public class JdbcParallelRead {
	
	private static final Logger LOG = LoggerFactory.getLogger(JdbcParallelRead.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws PropertyVetoException {
		
		
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		dataSource.setDriverClass("com.mysql.cj.jdbc.Driver");
		dataSource.setJdbcUrl("jdbc:mysql://google/employees?cloudSqlInstance=celtic-list-244219:us-central1:cdf01" +
				"&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false" +
				"&user=cdf01&password=cdf01");

		
	    FirestoreOptions firestoreOptions =
	        FirestoreOptions.getDefaultInstance().toBuilder()
	            .setProjectId("celtic-list-244219")
	            .build();
	    
	    Firestore db = firestoreOptions.getService();


		dataSource.setMaxPoolSize(10);
		dataSource.setInitialPoolSize(6);
		JdbcIO.DataSourceConfiguration config
		= JdbcIO.DataSourceConfiguration.create(dataSource);

		JPOptions options =
		        PipelineOptionsFactory.fromArgs(args).withValidation().as(JPOptions.class);
		
		Pipeline p = Pipeline.create(options);
		WriteBatch batch = db.batch();

		String tableName = "employees";
		int fetchSize = 1000;

		//    Create range index chunks Pcollection
		PCollection<KV<String,Iterable<Integer>>> ranges =
				p.apply(String.format("Read from Cloud SQL MySQL: %s",tableName), JdbcIO.<String>read()
						.withDataSourceConfiguration(config)
						.withQuery(String.format("SELECT MAX(`emp_no`) from %s", tableName))
						.withRowMapper(new JdbcIO.RowMapper<String>() {
							public String mapRow(ResultSet resultSet) throws Exception {
								return resultSet.getString(1);
							}
						})
						.withOutputParallelization(false)
						.withCoder(StringUtf8Coder.of()))
				.apply("Distribute", ParDo.of(new DoFn<String, KV<String, Integer>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						int readChunk = fetchSize;
						int count = Integer.parseInt((String) c.element());
						int ranges = (int) (count / readChunk);
						for (int i = 0; i < ranges; i++) {
							int indexFrom = i * readChunk;
							int indexTo = (i + 1) * readChunk;
							String range = String.format("%s,%s",indexFrom, indexTo);
							KV<String,Integer> kvRange = KV.of(range, 1);
							c.output(kvRange);
						}
						if (count > ranges * readChunk) {
							int indexFrom = ranges * readChunk;
							int indexTo = ranges * readChunk + count % readChunk;
							String range = String.format("%s,%s",indexFrom, indexTo);
							KV<String,Integer> kvRange = KV.of(range, 1);
							c.output(kvRange);
						}
					}
				}))
				.apply("Break Fusion", GroupByKey.create())
				;


		ranges.apply(String.format("Read ALL %s", tableName), JdbcIO.<KV<String,Iterable<Integer>>,Map<String, String>>readAll()
				.withDataSourceConfiguration(config)
				.withFetchSize(fetchSize)
				.withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String,Iterable<Integer>>>() {
					@Override

					public void setParameters(KV<String,Iterable<Integer>> element,
							PreparedStatement preparedStatement) throws Exception {

						String[] range = element.getKey().split(",");
						preparedStatement.setInt(1, Integer.parseInt(range[0]));
						preparedStatement.setInt(2, Integer.parseInt(range[1]));
					}
				})
				.withOutputParallelization(false)
				.withQuery(String.format("select * from employees.%s where emp_no >= ? and emp_no < ?",tableName))
				.withRowMapper((JdbcIO.RowMapper<Map<String, String>>) resultSet -> {
					Map<String, String> data = new HashMap<>();
					for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
	
						String columnTypeIntKey ="";
						try {
							data.put(resultSet.getMetaData().getColumnName(i), resultSet.getString(i));
						} catch (Exception e) {
							LOG.error("problem columnTypeIntKey: " +  columnTypeIntKey);
							throw e;
						}
					}

					return data;
				})
				)

		/*
		 * .apply("Build Document", MapElements.via(new SimpleFunction<Map<String,
		 * String>, Integer>() {
		 * 
		 * @Override public Integer apply(Map<String, String> data) { DocumentReference
		 * docRef =
		 * db.collection("employees").document(String.valueOf(data.get("emp_no")));
		 * batch.set(docRef, data); return data.size(); } }))
		 * .apply("Send to Firestore", MapElements.via( new SimpleFunction<Integer,
		 * Integer>() {
		 * 
		 * @Override public Integer apply(Integer line) { batch.commit(); return line; }
		 * }))
		 */
		;

		p.run();
	}
	
	  public interface JPOptions extends PipelineOptions {

		    /** Set this required option to specify where to write the output. */
		    @Description("Path of the file to write to")
		    @Required
		    String getOutput();

		    void setOutput(String value);
		  }
}

