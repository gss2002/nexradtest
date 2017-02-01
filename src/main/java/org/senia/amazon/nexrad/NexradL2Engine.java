package org.senia.amazon.nexrad;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class NexradL2Engine {
	public static Queue<String> msgQueueMap = new LinkedList<String>();
	public static Hashtable<String, ChunkProcessor> chunkQueueMap = new Hashtable<String, ChunkProcessor>();

	private static final Logger log = LoggerFactory.getLogger(NexradL2Engine.class);
	public static String nexradOutputPath;
	public static String l2refConfig;
	public static String l2bvConfig;
	public static String l2ccConfig;
	public static String l2zdrConfig;
	public static String l2kdpConfig;
	public static String l2swConfig;
	public static String radar_home;
	public static List<String> radar_list;

	public static String nexradQueueConfig;
	public static String gradsScript;

	public static void main(String[] args) {
		nexradOutputPath = System.getProperty("nexradOutputPath");
		l2refConfig = System.getProperty("l2refNexradConfig");
		l2bvConfig = System.getProperty("l2bvNexradConfig");
		l2ccConfig = System.getProperty("l2ccNexradConfig");
		l2zdrConfig = System.getProperty("l2zdrNexradConfig");
		l2kdpConfig = System.getProperty("l2kdpNexradConfig");
		l2swConfig = System.getProperty("l2swNexradConfig");
		radar_home = System.getProperty("radar_home");


		nexradQueueConfig = System.getProperty("nexradQueueConfig");

		long start = System.currentTimeMillis();
		// Optionally remove existing handlers attached to j.u.l root logger
		SLF4JBridgeHandler.removeHandlersForRootLogger(); // (since SLF4J 1.6.5)

		// add SLF4JBridgeHandler to j.u.l's root logger, should be done once
		// during
		// the initialization phase of your application
		SLF4JBridgeHandler.install();
		NexradMsg2S3LockAgent nexrLockAgent = new NexradMsg2S3LockAgent();
		log.info("NexradMsg2S3LockAgent Class: " + nexrLockAgent.getClass().getName());
		NexradChunkLockAgent nexrChunkLockAgent = new NexradChunkLockAgent();
		log.info("NexradChunkLockAgent Class: " + nexrChunkLockAgent.getClass().getName());

		NexradMessageWorker nexradMsgThread1 = new NexradMessageWorker("NexradMessageThread-1", nexradQueueConfig);
		NexradMessageWorker nexradMsgThread2 = new NexradMessageWorker("NexradMessageThread-2", nexradQueueConfig);
		NexradMessageWorker nexradMsgThread3 = new NexradMessageWorker("NexradMessageThread-3", nexradQueueConfig);
		NexradMessageWorker nexradMsgThread4 = new NexradMessageWorker("NexradMessageThread-4", nexradQueueConfig);
		NexradMessageWorker nexradMsgThread5 = new NexradMessageWorker("NexradMessageThread-5", nexradQueueConfig);
		NexradMessageWorker nexradMsgThread6 = new NexradMessageWorker("NexradMessageThread-6", nexradQueueConfig);

		NexradS3Worker nexradS3Worker = new NexradS3Worker();
		nexradS3Worker.setName("NexradS3Thread");
		nexradS3Worker.start();
		nexradMsgThread1.setName("NexradMessageThread-1");
		nexradMsgThread1.start();
		nexradMsgThread2.setName("NexradMessageThread-2");
		nexradMsgThread2.start();
		nexradMsgThread3.setName("NexradMessageThread-3");
		nexradMsgThread3.start();
		nexradMsgThread4.setName("NexradMessageThread-4");
		nexradMsgThread4.start();
		nexradMsgThread5.setName("NexradMessageThread-5");
		nexradMsgThread5.start();
		nexradMsgThread6.setName("NexradMessageThread-6");
		nexradMsgThread6.start();
		log.info("Finished starting threads");

		long end = System.currentTimeMillis();
		log.info("StartupTime: " + (end - start) + "ms");

	}

	public static void loadRadarList() {
		String radarfile = radar_home+"/conf/radar.sites";
		radar_list = new ArrayList<>();

		try (Stream<String> stream = Files.lines(Paths.get(radarfile))) {

			// 1. convert all content to upper case
			// 2. convert it into a List
			radar_list = stream.map(String::toUpperCase).collect(Collectors.toList());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
