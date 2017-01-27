package org.senia.nexrad.l2;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class NexradScanEngine {

	public static void main(String[] args) {
		File file = new File(args[0]);
		String outputdir = args[1];

		Collection<File> files = FileUtils.listFiles(file, null, true);
		int elev = 0;

		int vcp = 0;
		int radialCount = 0;
		File headerFile = null;
		String siteId = null;

		float deg_elev = 0;
		// Map<String,Float> tiltMap = new HashMap<String, Float>();
		List<Float> tiltList = new ArrayList<Float>();
		int tempRadialCount = 0;
		List<File> elevFileList = new ArrayList<File>();

		for (File file2 : files) {
			NexradFileEngine nfe = new NexradFileEngine();
			if (file2.getName().endsWith("001-S")) {
				headerFile = file2;
			}
			try {
				// System.out.println("File: "+file2.getAbsolutePath());
				nfe.testValidScan(file2.getAbsolutePath(), outputdir);
				if (siteId == null) {
					siteId = nfe.siteId;
					System.out.println("siteId: "+siteId);

				}
				if (nfe.vcp != 0) {
					if (nfe.firstRadial == 1 && tempRadialCount == 0) {
						tempRadialCount = 0;
						if (vcp == 0) {
							vcp = nfe.vcp;
						}
						if (!elevFileList.isEmpty()) {
							elevFileList.clear();
						}
						elevFileList.add(headerFile);
						if (nfe.elev != elev) {
							elev = nfe.elev;
						}
						if (nfe.elevang != deg_elev) {
							deg_elev = nfe.elevang;
						}
						if (nfe.hasHighResolutionPHI && nfe.hasHighResolutionREF && nfe.hasHighResolutionRHO
								&& nfe.hasHighResolutionSW && nfe.hasHighResolutionVEL && nfe.hasHighResolutionZDR) {
							radialCount = 360;
						} else {
							radialCount = 1440;
						}
						elevFileList.add(file2);
						tempRadialCount = tempRadialCount + nfe.radialCount;
					} else {
						elevFileList.add(file2);
						tempRadialCount = tempRadialCount + nfe.radialCount;
					}

					if (tempRadialCount == radialCount) {
						System.out.println("elevFileList: " + elevFileList);
						//File[] fileList = (File[]) elevFileList.toArray();
						File[] fileList = new File[elevFileList.size()]; 
						elevFileList.toArray(fileList);
						String filename = siteId+"-"+elev;
						joinFiles(new File(filename), fileList);
						System.out.println(
								"FILE: " + file2.getName() + " VCP: " + vcp + " Elev.: " + elev + " Elev Angle: "
										+ deg_elev + "  Radials" + radialCount + ": " + tempRadialCount + " Complete");
						tempRadialCount = 0;
						if (!elevFileList.isEmpty()) {
							elevFileList.clear();
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void joinFiles(File destination, File[] sources) throws IOException {
		DataOutputStream output = null;
		try {
			output = createAppendableStream(destination);
			for (File source : sources) {
				System.out.println("Merging: "+source);
				appendFile(output, source);
			}
			output.close();
		} finally {
			IOUtils.closeQuietly(output);
		}
	}

	private static DataOutputStream createAppendableStream(File destination) throws FileNotFoundException {
		return new DataOutputStream(new FileOutputStream(destination, true));
	}

	private static void appendFile(OutputStream output, File source) throws IOException {
		DataInputStream dataIs = null;
		try {
			byte[] dataFile = new byte[(int)source.length()];
			dataIs = new DataInputStream(new FileInputStream(source));
			dataIs.readFully(dataFile);
		    IOUtils.write(dataFile, output);
		    dataIs.close();
		} finally {
			IOUtils.closeQuietly(dataIs);
		}
	}

	public static Integer getVCPElev(int code) {
		switch (code) {
		case 11:
			return 16;
		case 12:
			return 14;
		case 21:
			return 11;
		case 31:
			return 8;
		case 32:
			return 7;
		case 35:
			return 9;
		case 121:
			return 9;
		case 211:
			return 14;
		case 212:
			return 14;
		case 215:
			return 15;
		case 221:
			return 9;
		default:
			return 0;
		}
	}

	public static Integer getVCPElevScans(int code) {
		switch (code) {
		case 11:
			return 16;
		case 12:
			return 14;
		case 21:
			return 11;
		case 31:
			return 8;
		case 32:
			return 7;
		case 121:
			return 20;
		case 211:
			return 16;
		case 212:
			return 17;
		case 221:
			return 11;
		default:
			return 0;
		}
	}

}
