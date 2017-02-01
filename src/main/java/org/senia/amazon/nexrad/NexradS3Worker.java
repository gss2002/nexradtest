package org.senia.amazon.nexrad;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import ucar.nc2.util.DiskCache;

public class NexradS3Worker extends Thread implements Runnable {
	public AwsS3 nexradS3;
	private static final Logger log = LoggerFactory.getLogger(NexradS3Worker.class);
	private String nexradOutputPath = NexradL2Engine.nexradOutputPath;
	private String l2refConfig = NexradL2Engine.l2refConfig;
	private String l2bvConfig = NexradL2Engine.l2bvConfig;
	private String l2ccConfig = NexradL2Engine.l2ccConfig;
	private String l2zdrConfig = NexradL2Engine.l2zdrConfig;
	private String l2kdpConfig = NexradL2Engine.l2kdpConfig;
	private String l2swConfig = NexradL2Engine.l2swConfig;
	private String radar_home = NexradL2Engine.radar_home;

	public NexradS3Worker() {
		nexradS3 = new AwsS3();
	}

	public void run() {
		log.debug("Nexrad S3 Worker starting");
		while (true) {
			log.trace("Nexrad S3 Worker looking for files to download");
			Iterator<String> it = NexradL2Engine.msgQueueMap.iterator();
			while (it.hasNext()) {
				String nexradPath = NexradL2Engine.msgQueueMap.poll();
				String site = nexradPath.split("/")[0];
				String scan = nexradPath.split("/")[1];
				
				try {
					log.debug("Downloading: "+site+" "+nexradPath);
					File file = nexradS3.downloadNexrad(nexradPath, nexradOutputPath);
					String nexr = file.toString();
					ChunkProcessor chunkProc = null;
					if (!(NexradL2Engine.chunkQueueMap.containsKey(site))){
						chunkProc = new ChunkProcessor(site, scan);
						NexradL2Engine.chunkQueueMap.put(site, chunkProc);
						chunkProc.start();
					} else {
						if (!(NexradL2Engine.chunkQueueMap.get(site).nexradScan.equals(scan))) {
							NexradL2Engine.chunkQueueMap.get(site).nexradScan = scan;
						}
						chunkProc = NexradL2Engine.chunkQueueMap.get(site);
						synchronized (chunkProc.lockObj) {
							chunkProc.lockObj.notify();
						}
					}
					
					
					
					/*String nexrout = nexr.split(".ar2v")[0];
					WctExporter wct = new WctExporter();
					wct.convert2NC(nexr, nexrout + ".ref", l2refConfig);
					wct.convert2NC(nexr, nexrout + ".bv", l2bvConfig);
					wct.convert2NC(nexr, nexrout + ".kdp", l2kdpConfig);
					wct.convert2NC(nexr, nexrout + ".zdr", l2zdrConfig);
					wct.convert2NC(nexr, nexrout + ".cc", l2ccConfig);
					wct.convert2NC(nexr, nexrout + ".sw", l2swConfig);
					File deleteNexrFile = new File(nexr);
					deleteNexrFile.delete();
					deleteWctCacheFile(nexr);

					String[] command = { radar_home + "/radar_scripts/radar.sh", site, nexrout };
					ProcessBuilder probuilder = new ProcessBuilder(command);
					// You can set up your work directory
					Process process = probuilder.start();
					process.isAlive();
					// Read out dir output
					InputStream is = process.getInputStream();
					InputStreamReader isr = new InputStreamReader(is);
					BufferedReader br = new BufferedReader(isr);
					String line;
					log.info("Output of running %s is:\n", Arrays.toString(command));
					while ((line = br.readLine()) != null) {
						log.info(line);
					}

					// Wait to get exit value
					try {
						int exitValue = process.waitFor();
						log.info("\n\nExit Value is " + exitValue);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					File deleteNexrRefFile = new File(nexrout+".ref.nc");
					deleteNexrRefFile.delete();
					File deleteNexrBvFile = new File(nexrout+".bv.nc");
					deleteNexrBvFile.delete();
					File deleteNexrkdpFile = new File(nexrout+".kdp.nc");
					deleteNexrkdpFile.delete();
					File deleteNexrzdrFile = new File(nexrout+".zdr.nc");
					deleteNexrzdrFile.delete();
					File deleteNexrccFile = new File(nexrout+".cc.nc");
					deleteNexrccFile.delete();
					File deleteNexrswFile = new File(nexrout+".sw.nc");
					deleteNexrswFile.delete();
					*/
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				

			}
			synchronized (NexradMsg2S3LockAgent.lockObj) {
				try {
					log.debug("Putting Nexrad S3 Worker into wait");
					NexradMsg2S3LockAgent.lockObj.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				}
			}
		}
	}
	
	public void deleteWctCacheFile(String nexr) {
		String nexrFile = null;
		try {
			String cacheDir = DiskCache.getRootDirectory();
			nexrFile = URLEncoder.encode(nexr, "UTF-8");
			nexrFile = cacheDir+"/"+nexrFile+".uncompress";
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (nexrFile != null) {
			File nexradFile = new File(nexrFile); 
			if (nexradFile.exists()) {
				nexradFile.delete();				
			}
		}
		
	}

}
