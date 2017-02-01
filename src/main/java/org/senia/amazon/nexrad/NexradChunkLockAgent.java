package org.senia.amazon.nexrad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NexradChunkLockAgent implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(NexradChunkLockAgent.class);

	static final Object lockObj = new Object();
	static final Object[] workerThreadLock = new Object[50];

	public NexradChunkLockAgent() {
		super();
		for (int x = 0; x <= 49; x++) {
			workerThreadLock[x] = new Object();
			log.info("NexradChunkLockAgent", "NexradChunkLockAgent Object: " + x);
		}
		log.info("NexradChunkLockAgent Objects Created");

		// super();
		// qName = aName;

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
}
