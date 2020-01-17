package com.slalom.gcp.dataflow.example;

import java.util.Map;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;

public class FireStoreIO {

	private static final Logger LOG = LoggerFactory.getLogger(FireStoreIO.class);


	FireStoreIO() {

	}

	public static <T> Write<T> write(String projectId, String collectionId) {
		return new Write<T>(projectId, collectionId);
	}

	public  static class Write<T> extends PTransform<PCollection<T>, PCollection<Void>> {

		static final long serialVersionUID = 1L;
		static final long DEFAULT_BATCH_SIZE = 1000L;

		ValueProvider<String> projectId;
		String collectionId;

		Write(String projectId, String collectionId) {
			this.projectId = ValueProvider.StaticValueProvider.of(projectId);
			this.collectionId = collectionId;
		}

		public ValueProvider<String> getProjectId() {
			return projectId;
		}

		public String getCollectionId() {
			return collectionId;
		}

		@Override
		public void populateDisplayData(DisplayData.Builder builder) {
			super.populateDisplayData(builder);
			builder
			.addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("ProjectId"))
			.addIfNotNull(DisplayData.item("collectionId", getCollectionId()).withLabel("CollectionId"));
		}

		@Override
		public PCollection<Void> expand(PCollection<T> input) {
			return input.apply(ParDo.of(new WriteFn<>(this)));
		}

	}

	private static class WriteFn<T> extends DoFn<T, Void> {


		private static final long serialVersionUID = 1L;

		final Write<T> spec;
		Firestore db;
		WriteBatch batch;
		int count = 0;

		public WriteFn(Write<T> spec) {
			this.spec = spec;
		}

		@Setup
		public void setup() {

			FirestoreOptions firestoreOptions =
					FirestoreOptions.getDefaultInstance().toBuilder()
					.setProjectId(spec.getProjectId().get())
					.build();

			db = firestoreOptions.getService();	
		}

		@StartBundle
		public void startBundle() throws Exception {
			batch = db.batch();

		}

		@ProcessElement
		public void processElement(ProcessContext context) throws Exception {

			T record = context.element();
			
			LOG.info("Date: " + (String)record);

			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> map = mapper.readValue((String)record, new TypeReference<Map<String,String>>(){});

			DocumentReference docRef =
					db.collection(spec.getCollectionId()).document("Name:" + String.valueOf(Math.random()));

			batch.set(docRef, map);
			count++;

			//if (count >= Write.DEFAULT_BATCH_SIZE) {
				
				batch.commit();
			//}
			count = 0;

		}

	}
}