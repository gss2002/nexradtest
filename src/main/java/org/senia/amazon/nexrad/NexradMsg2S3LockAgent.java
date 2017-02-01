package org.senia.amazon.nexrad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NexradMsg2S3LockAgent implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(NexradMsg2S3LockAgent.class);

	static final Object lockObj = new Object();
	static final Object[] workerThreadLock = new Object[50];

	public NexradMsg2S3LockAgent() {
		super();
		for (int x = 0; x <= 49; x++) {
			workerThreadLock[x] = new Object();
			log.info("NexradMsg2S3LockAgent", "NexradMsg2S3LockAgent Object: " + x);
		}
		log.info("NexradMsg2S3LockAgent Objects Created");

		// super();
		// qName = aName;

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
}
