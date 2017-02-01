package org.senia.nexrad.l2;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import ucar.unidata.io.RandomAccessFile;

public class NexradScanEngine {

	public static void main(String[] args) {
		File file = new File(args[0]);
		String outputdir = args[1];

		List<File> files = (List<File>) FileUtils.listFiles(file, null, true);
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

		// for (File file2 : files) {
		for (int i = 0; i < files.size(); i++) {
			File file2 = files.get(i);
			NexradFileEngine nfe = new NexradFileEngine();
			if (file2.getName().endsWith("001-S")) {
				headerFile = file2;
			}
			try {
				// System.out.println("File: "+file2.getAbsolutePath());
				nfe.testValidScan(file2.getAbsolutePath(), outputdir);
				if (siteId == null) {
					siteId = nfe.siteId;
					System.out.println("siteId: " + siteId);

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
						if (i != files.size() - 1) {
							elevFileList.add(files.get(i + 1));
						}
						File[] fileList = new File[elevFileList.size()];
						elevFileList.toArray(fileList);
						String filename = siteId + "-" + elev;
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
		for (File source : sources) {
			System.out.println("Merging: " + source);
			appendFile(destination, source);
		}
	}

	private static void appendFile(File output, File source) throws IOException {
		RandomAccessFile raf = ucar.unidata.io.RandomAccessFile.acquire(source.getPath());
		RandomAccessFile outRaf;
		outRaf = new ucar.unidata.io.RandomAccessFile(output.getPath(), "rw");
		long outRafSize = outRaf.length();
		outRaf.seek(outRafSize);
		byte[] dataFile = new byte[(int) source.length()];
		raf.readFully(dataFile);
		outRaf.write(dataFile);
		outRaf.close();
		raf.close();

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
