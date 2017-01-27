package org.senia.amazon.nexrad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QueueMonitor implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(QueueMonitor.class);

	static final Object lockObj = new Object();
	static final Object[] workerThreadLock = new Object[50];

	public QueueMonitor() {
		super();
		for (int x = 0; x <= 49; x++) {
			workerThreadLock[x] = new Object();
			log.info("QueueMonitor", "QueueMonitor ThreadLock Object: " + x);
		}
		log.info("QueueMonitor Worker Thread Objects Created");

		// super();
		// qName = aName;

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
}
