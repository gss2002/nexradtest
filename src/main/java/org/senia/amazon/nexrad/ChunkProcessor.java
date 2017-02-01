package org.senia.amazon.nexrad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkProcessor extends Thread implements Runnable {
	public String nexradSite;
	public String nexradScan;
	public final Object lockObj = new Object();

	private static final Logger log = LoggerFactory.getLogger(ChunkProcessor.class);

	public ChunkProcessor(String site, String nexradScan) {
		this.nexradSite = site;
		this.nexradScan = nexradScan;
		log.debug("NexradSite Initial: " + nexradSite);
		log.debug("Nexrad VSCAN Initial: " + nexradScan);

	}

	public void run() {
		while (true) {
			log.debug("NexradSite: " + nexradSite);
			log.debug("Nexrad VSCAN: " + nexradScan);
			synchronized (lockObj) {
				try {
					log.debug("Putting Nexrad ChunkProcessor Worker into wait");
					lockObj.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				}
			}
		}
	}
}
