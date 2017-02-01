package org.senia.amazon.nexrad;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class NexradMessageWorker extends Thread implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(NexradMessageWorker.class);
	private final Object lock = new Object();
	private static String QUEUE = "NEXRChunkL2Queue";
	private static String accessKey = "";
	private static String secretKey = "";

	private String command;

	public NexradMessageWorker(String s, String propPath) {
		NexradPropReader nexPropRdr = new NexradPropReader();
		Properties nexrProps = null;
		try {
			nexrProps = nexPropRdr.getNexradConfig(propPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		accessKey = nexrProps.getProperty("accessKey");
		secretKey = nexrProps.getProperty("secretKey");
		QUEUE = nexrProps.getProperty("queueKey");
		log.info("Amazon QueueName: " + QUEUE);
		this.command = s;
	}

	@Override
	public void run() {
		synchronized (lock) {
			log.info(Thread.currentThread().getName() + " Start. Command = " + command);
			while (true) {
				getMessages();
				try {
					log.debug("Backoff - waiting for messages");
					lock.wait(5000L);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private void getMessages() {
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);

			ClientConfiguration cc = new ClientConfiguration();
			AmazonSQS sqs = new AmazonSQSClient(awsCreds, cc);
			sqs.setRegion(Region.getRegion(Regions.US_EAST_1));

			String queueUrl = sqs.createQueue(QUEUE).getQueueUrl();
			log.debug("QueueURL: "+queueUrl);
			ReceiveMessageRequest request = new ReceiveMessageRequest(QUEUE).withQueueUrl(queueUrl);
			request.setVisibilityTimeout(5);
			boolean queueExists = true;
			while (queueExists) {
				NexradL2Engine.loadRadarList();
				ReceiveMessageResult result = sqs.receiveMessage(request);
				log.debug("Queue Messages Empty: "+result.getMessages().isEmpty());
				if (result.getMessages().isEmpty()) {
					log.debug("Empty Messages");
					queueExists = false;
				} else {
					Log.debug("Size: "+result.getMessages().size());
					result.getMessages().stream().forEach(s -> {
						String body = s.getBody();
						String path = getPath(body);
						String site = path.split("/")[0];
						log.debug("Key: " + path);
						List<String> radarlist = NexradL2Engine.radar_list;
						if (radarlist.contains(site)) {
							log.debug("Site: " + site);
						//if (site.equalsIgnoreCase("KOKX") || site.equalsIgnoreCase("KDOX") || site.equalsIgnoreCase("KBOX") || site.equalsIgnoreCase("KOAX")) {
							NexradL2Engine.msgQueueMap.add(path);
							synchronized (NexradMsg2S3LockAgent.lockObj) {
								NexradMsg2S3LockAgent.lockObj.notify();
							}
						}
						deleteMessage(sqs, s);
					});
				}
			}
			sqs.shutdown();
		} catch (Exception ae) {
			ae.printStackTrace();

		}
	}

	private void deleteMessage(AmazonSQS sqs, Message message) {

		DeleteMessageRequest deleteRequest = new DeleteMessageRequest();
		deleteRequest.setQueueUrl(sqs.createQueue(QUEUE).getQueueUrl());
		deleteRequest.setReceiptHandle(message.getReceiptHandle());
		sqs.deleteMessage(deleteRequest);
	}

	public static String getPath(String message) {
		// Find instance of "key":
		int keyIdx = message.indexOf("key\\\"");
		String s = message.substring(keyIdx);
		int startIdx = s.indexOf("\\\"");
		s = s.substring(startIdx + 5);
		int stopIdx = s.indexOf("\\\"");
		s = s.substring(0, stopIdx);

		return s;
	}
}
