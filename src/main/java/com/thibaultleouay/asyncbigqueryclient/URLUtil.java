package com.thibaultleouay.asyncbigqueryclient;

public class URLUtil {

	public static String generateDatasetsUrl(String projectId, String datasetId) {

		return "/projects/" + projectId + "/datasets/" + datasetId;
	}

	public static String generateJobUrl(String projectId, String jobId) {
		return "/projects/" + projectId + "/jobs/" + jobId;
	}

	public static String generateQueryUrl(String projectId, String jobId) {
		return "/projects/" + projectId + "/queries/" + jobId;

	}

	public static String generateTableURL(String projectId, String datasetId, String tableId) {
		return "projects/" + projectId + "/datasets/" + datasetId + "/tables/" + tableId ;
	}
}
