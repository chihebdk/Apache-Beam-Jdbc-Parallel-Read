/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.firestore;

import com.google.auto.value.AutoValue;
import com.google.cloud.firestore.FirestoreOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import javax.annotation.Nullable;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class FirestoreV1 {

	// A package-private constructor to prevent direct instantiation from outside of this package
	FirestoreV1() {
	}

	public Write<?> write() {
		return new Write.Builder().build();
	}



	public  static class Write<T> extends PTransform<PCollection<T>, PDone> {

		private static final long serialVersionUID = 1L;

		ValueProvider<String> projectId;
		String collectionId;
		String documentId;

		
		
		public ValueProvider<String> getProjectId() {
			return projectId;
		}

		public void setProjectId(ValueProvider<String> getProjectId) {
			this.projectId = getProjectId;
		}

		public String getCollectionId() {
			return collectionId;
		}

		public void setCollectionId(String collectionId) {
			this.collectionId = collectionId;
		}

		public String getDocumentId() {
			return documentId;
		}

		public void setDocumentId(String documentId) {
			this.documentId = documentId;
		}




		Builder<T> toBuilder() {

			return null;
		}

		@Override
		public String toString() {
			return "Write [getProjectId=" + projectId + ", collectionId=" + collectionId + ", documentId="
					+ documentId + "]";
		}

		@AutoValue.Builder
		public static class Builder<T> {

			Builder() {}

			Builder<T> setProjectId(ValueProvider<String> projectId) {
				return null;
			}

			Builder<T> setCollectionId(String collectionId) {
				return null;
			}

			Builder<T> setDocumentId(String documentId) {
				return null;
			}

			 Write<T> build() {
				return null;
			}
		}

		/**
		 * Returns a new {@link Write} that reads from the Cloud Firestore for the specified project.
		 */
		public Write<T> withProjectId(String projectId) {
			checkArgument(projectId != null, "projectId can not be null");
			return toBuilder().setProjectId(ValueProvider.StaticValueProvider.of(projectId)).build();
		}

		/**
		 * Same as {@link Write#withProjectId(String)} but with a {@link ValueProvider}.
		 */
		public Write<T> withProjectId(ValueProvider<String> projectId) {
			checkArgument(projectId != null, "projectId can not be null");
			return toBuilder().setProjectId(projectId).build();
		}

		/**
		 * Returns a new {@link Write} that reads from the Cloud Firestore for the specified collection identifier.
		 */
		public Write<T> to(String collectionId) {
			checkArgument(collectionId != null, "collectionId can not be null");
			return toBuilder().setCollectionId(collectionId).build();
		}

		/**
		 * Returns a new {@link Write} that reads from the Cloud Firestore for the specified document identifier.
		 */
		public Write<T> withDocumentId(String documentId) {
			checkArgument(documentId != null, "documentId can not be null");
			return toBuilder().setDocumentId(documentId).build();
		}

		@Override
		public void populateDisplayData(DisplayData.Builder builder) {
			super.populateDisplayData(builder);
			builder
			.addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("ProjectId"))
			.addIfNotNull(DisplayData.item("collectionId", getCollectionId()).withLabel("CollectionId"))
			.addIfNotNull(DisplayData.item("documentId", getDocumentId()).withLabel("DocumentId"));
		}

		@Override
		public PDone expand(PCollection<T> input) {
			input.apply("Write to Firestore", ParDo.of(
					new FirestoreWriterFn<T>(
							getProjectId().get(),
							getCollectionId(),
							getDocumentId(),
							new FirestoreOptions.DefaultFirestoreFactory(),
							new WriteBatcherImpl(),
							new FirestoreBatchRequesterFactory<T>())));
			return PDone.in(input.getPipeline());
		}
	}

	
}
