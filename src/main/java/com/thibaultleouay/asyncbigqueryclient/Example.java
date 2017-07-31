package com.thibaultleouay.asyncbigqueryclient;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.JobList.Jobs;



public class Example {
	ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public static void main(String[] args) {

		BigQueryBuilder builder = new BigQueryBuilder();
		BigQuery bigQuery = BigQuery.create(builder);
		try {
			Dataset result = bigQuery.getDataset("bigquery-public-data", "hacker_news").get();
			result.getDescription();
			System.out.println(result);
			System.out.println(result.getId());
			// String content = new
			// String(Files.readAllBytes(Paths.get("src/main/resources/datasets.json")));

			// Gson gson = new GsonBuilder().create();
			// Datasets other = gson.fromJson(content, Datasets.class);

			// Datasets otherResult = bigQuery.insertDataset("databerries-staging",
			// other).get();
			// System.out.println(otherResult.getId());
			//
			// bigQuery.deleteDataset("databerries-staging", "test");

			// Datasets patch = new Datasets();
			// patch.setDescription("patch description");
			// Datasets patchResult = bigQuery.patchDataset("databerries-staging", "test",
			// patch).get();

			// other.setDescription("update test");
			// Datasets patchResult = bigQuery.updateDataset("databerries-staging", "test",
			// other).get();
			//
			// DatasetsList list = bigQuery.listDataset("databerries-staging").get();
			// System.out.println(list);

			
//			Jobs job = bigQuery.getJob("databerries-staging", "job_EYG3NfjaIC9jobZO5hy4fXV3CS0").get();
//			BigQueryFuture<Jobs> a = bigQuery.getJob("databerries-staging", "job_EYG3NfjaIC9jobZO5hy4fXV3CS0");
//			BigQueryFuture<Jobs> b = bigQuery.getJob("databerries-staging", "job_EYG3NfjaIC9jobZO5hy4fXV3CS0");
//			bigQuery.getJob("databerries-staging", "job_EYG3NfjaIC9jobZO5hy4fXV3CS0").thenApply( s -> bigQuery.cancelJob("", s.getId()));
			bigQuery.close();

			
		} catch (InterruptedException | ExecutionException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}
	
}
