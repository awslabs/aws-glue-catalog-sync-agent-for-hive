package com.amazonaws.services.glue.catalog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogGroupResult;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;
import com.amazonaws.services.logs.model.ResourceNotFoundException;

class CloudWatchLogsReporter {
	private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsReporter.class);

	private final String LOG_GROUP_NAME = "HIVE_METADATA_SYNC";
	private String uploadSeqToken;
	private String LOG_STREAM_NAME = "";
	// private AWSLogs awsLogs = AWSLogsClientBuilder.standard().withCredentials(new
	// com.amazonaws.auth.InstanceProfileCredentialsProvider()).build();
	private AWSLogs awsLogs;

	public CloudWatchLogsReporter(Configuration config) {
		// connect to cloudwatch logs in the region as specified by the configuration
		// or the environment
		AWSLogsClientBuilder logBuilder = AWSLogsClientBuilder.standard();
		String hadoopConfigRegion = config.get("AWS_REGION", null);
		String setRegion = hadoopConfigRegion == null ? System.getProperty("AWS_REGION") : hadoopConfigRegion;
		logBuilder.setRegion(setRegion == null ? "us-east-1" : setRegion);
		awsLogs = logBuilder.build();

		// set the log stream name to local host, and override with configured log
		// stream name if there is one
		try {
			LOG_STREAM_NAME = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			LOG_STREAM_NAME = "DEFAULT_HOSTNAME";
		}
		String configLogStream = config.get("LOG_STREAM_NAME, null");

		if (configLogStream != null) {
			LOG_STREAM_NAME = configLogStream;
		}

		boolean isFoundLogStream = false;
		// DescribeLogGroupsRequest describeLogGroupsRequest = new
		// DescribeLogGroupsRequest();
		// describeLogGroupsRequest.setLogGroupNamePrefix(LOG_GROUP_NAME);
		// DescribeLogGroupsResult describeLogGroupsResult =
		// awsLogs.describeLogGroups(describeLogGroupsRequest);
		// if (describeLogGroupsResult.getLogGroups().size() == 0) {
		// CreateLogGroupResult logGroupResult = awsLogs.createLogGroup(new
		// CreateLogGroupRequest(LOG_GROUP_NAME));
		// }

		DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest(LOG_GROUP_NAME);
		describeLogStreamsRequest.setLogStreamNamePrefix(LOG_STREAM_NAME);

		List<LogStream> logStreams = null;

		try {
			logStreams = awsLogs.describeLogStreams(describeLogStreamsRequest).getLogStreams();
		} catch (ResourceNotFoundException e) {
			// Creating Log group
			CreateLogGroupResult logGroupResult = awsLogs.createLogGroup(new CreateLogGroupRequest(LOG_GROUP_NAME));
			logStreams = awsLogs.describeLogStreams(describeLogStreamsRequest).getLogStreams();
		}

		if (logStreams.size() != 0) {
			Iterator<LogStream> lsIterator = logStreams.iterator();

			while (lsIterator.hasNext() && !isFoundLogStream) {
				LogStream currLS = lsIterator.next();
				if (currLS.getLogStreamName().equals(LOG_STREAM_NAME)) {
					isFoundLogStream = true;
					this.uploadSeqToken = currLS.getUploadSequenceToken();
				}
			}
		}

		if (logStreams.size() == 0 || !isFoundLogStream) {
			awsLogs.createLogStream(new CreateLogStreamRequest(LOG_GROUP_NAME, LOG_STREAM_NAME));
		}
	}

	public void sendToCWL(String message) {
		LOG.debug("******* Reporting to CWL: " + message);
		List<InputLogEvent> events = new ArrayList<>();
		InputLogEvent logEvent = new InputLogEvent().withMessage(message).withTimestamp(System.currentTimeMillis());
		events.add(logEvent);

		PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest(LOG_GROUP_NAME, LOG_STREAM_NAME, events)
				.withSequenceToken(this.uploadSeqToken);
		PutLogEventsResult putLogEventsResult = awsLogs.putLogEvents(putLogEventsRequest);
		this.uploadSeqToken = putLogEventsResult.getNextSequenceToken();
	}
}
