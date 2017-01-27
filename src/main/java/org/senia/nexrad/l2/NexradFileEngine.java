package org.senia.nexrad.l2;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;

import ucar.unidata.io.RandomAccessFile;
import ucar.unidata.io.bzip2.BZip2ReadException;
import ucar.unidata.io.bzip2.CBZip2InputStream;

public class NexradFileEngine {

	public int vcp = 0;
	public int cut = 0;
	public int radial = 0;
	public int firstRadial = 0;
	public int lastRadial = 0;
	public int radialCount = 0;
	public int elev = 0;
	public float elevang = 0;
	public String siteId = null;

	private String dataFormat = null; // ARCHIVE2 or AR2V0001
	private int title_julianDay; // days since 1/1/70
	private int title_msecs; // milliseconds since midnight
	public String stationId; // 4 letter station assigned by ICAO
	private NexradStationDB.Station station; // from lookup table, may be
												// null
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NexradFileEngine.class);
	public static final String ARCHIVE2 = "ARCHIVE2";
	public static final String AR2V0001 = "AR2V0001";
	public static final String AR2V0002 = "AR2V0002";
	public static final String AR2V0003 = "AR2V0003";
	public static final String AR2V0004 = "AR2V0004";
	public static final String AR2V0006 = "AR2V0006";
	public static final String AR2V0007 = "AR2V0007";
	public static final int FILE_HEADER_SIZE = 24;

	public RandomAccessFile uraf = null;
	private Level2Record first, last;

	private int max_radials = 0;
	private int min_radials = Integer.MAX_VALUE;
	private int max_radials_hr = 0;
	private int min_radials_hr = Integer.MAX_VALUE;
	private int dopplarResolution;
	private boolean hasDifferentDopplarResolutions;
	private boolean hasHighResolutionData;

	public boolean hasHighResolutionREF;
	public boolean hasHighResolutionVEL;
	public boolean hasHighResolutionSW;
	public boolean hasHighResolutionZDR;
	public boolean hasHighResolutionPHI;
	public boolean hasHighResolutionRHO;

	private List<List<Level2Record>> reflectivityGroups;
	private List<List<Level2Record>> dopplerGroups;
	private List<List<Level2Record>> reflectivityHighResGroups;
	private List<List<Level2Record>> velocityHighResGroups;
	private List<List<Level2Record>> spectrumHighResGroups;

	private List<List<Level2Record>> diffReflectHighResGroups;
	private List<List<Level2Record>> diffPhaseHighResGroups;
	private List<List<Level2Record>> coefficientHighResGroups;

	private boolean showMessages = false, showData = true, debugScans = true, debugGroups2 = false;
	private boolean debugRadials = true;
	private boolean runCheck = false;

	public void saveScan() {

	}

	public void testValidScan(String fileName, String outputDir) throws IOException {
		NexradStationDB.init();
		RandomAccessFile raf = null;
		String realFileName = null;
		// TODO Auto-generated method stub
		try {
			raf = ucar.unidata.io.RandomAccessFile.acquire(new File(fileName).getPath());
			File outputdir = new File("/tmp/"+outputDir);
			if (!(outputdir.exists())){
				new File("/tmp/"+outputDir).mkdirs();
			}
			realFileName = new File(fileName).getName();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		raf.seek(0);
		raf.order(RandomAccessFile.BIG_ENDIAN);
		// volume scan header
		dataFormat = raf.readString(8);
		raf.skipBytes(4);
		title_julianDay = raf.readInt(); // since 1/1/70
		title_msecs = raf.readInt();
		stationId = raf.readString(4).trim(); // only in AR2V0001
		// if (log.isDebugEnabled())
		//log.debug(" dataFormat= " + dataFormat + " stationId= " + stationId);

		if (stationId.length() == 0) {
			// try to get it from the filename LOOK

			stationId = null;
		}

		// try to find the station
		if (stationId != null) {
			if (!stationId.startsWith("K") && stationId.length() == 4) {
				String _stationId = "K" + stationId;
				station = NexradStationDB.get(_stationId);
			} else
				station = NexradStationDB.get(stationId);
		}

		// see if we have to uncompress the first chunk/file of the scan
		if (dataFormat.equals(AR2V0001) || dataFormat.equals(AR2V0003) || dataFormat.equals(AR2V0004)
				|| dataFormat.equals(AR2V0006) || dataFormat.equals(AR2V0007)) {
			raf.skipBytes(4);
			String BZ = raf.readString(2);
			//log.info(BZ);
			if (BZ.equals("BZ")) {
				File uncompressedFile = new File("/tmp/"+outputDir+"/"+realFileName+ ".uncompress");
				uraf = uncompress(raf, uncompressedFile.getPath());
				uraf.order(RandomAccessFile.BIG_ENDIAN);
				uraf.seek(Level2Record.FILE_HEADER_SIZE);
			}
			// all other files from scan go through the chunkDecompressor
		} else {
			raf.seek(0);
			raf.order(RandomAccessFile.BIG_ENDIAN);
			raf.skipBytes(4);
			String BZ = raf.readString(2);
			//log.info(BZ);
			if (BZ.equals("BZ")) {
				File uncompressedFile = new File("/tmp/"+outputDir+"/"+realFileName+ ".uncompress");
				uraf = uncompressChunk(raf, uncompressedFile.getPath());
				uraf.order(RandomAccessFile.BIG_ENDIAN);
			}
		}
		long message_offset31 = 0;
		int recno = 0;
		radialCount = 0;
		while (true) {
			Level2Record r = Level2Record.factory(uraf, recno++, message_offset31);
			if (r == null) {
				break;
			}
			if (r.message_type == 31) {
				// r.dumpMessage(System.out);
				message_offset31 = message_offset31 + (r.message_size * 2 + 12 - 2432);
				radialCount = radialCount +1;
			}
			if (this.vcp == 0) {
				this.vcp = r.vcp;
			}
			if (this.cut == 0) {
				this.cut = r.cut;
			}
			if (this.firstRadial == 0) {
				this.siteId = r.id;
				this.firstRadial = r.radial_num;
				this.hasHighResolutionREF = r.hasHighResREFData;
				this.hasHighResolutionPHI = r.hasHighResPHIData;
				this.hasHighResolutionRHO = r.hasHighResRHOData;
				this.hasHighResolutionSW = r.hasHighResSWData;
				this.hasHighResolutionVEL = r.hasHighResVELData;
				this.hasHighResolutionZDR = r.hasHighResZDRData;
			} else {
				this.lastRadial = r.radial_num;
			}
			
			if (this.elevang == 0) {
				this.elevang = r.elevation;
			}
			if (this.elev == 0) {
				this.elev = r.elevation_num;
			}


		}
		raf.close();
		uraf.close();
	}

	/**
	 * Write equivilent uncompressed version of the file.
	 *
	 * @param inputRaf
	 *            file to uncompress
	 * @param ufilename
	 *            write to this file
	 * @return raf of uncompressed file
	 * @throws IOException
	 *             on read error
	 */
	private static RandomAccessFile uncompress(RandomAccessFile inputRaf, String ufilename) throws IOException {
		RandomAccessFile outputRaf = new RandomAccessFile(ufilename, "rw");
		FileLock lock;

		while (true) { // loop waiting for the lock
			try {
				lock = outputRaf.getRandomAccessFile().getChannel().lock(0, 1, false);
				break;

			} catch (OverlappingFileLockException oe) { // not sure why lock()
														// doesnt block
				try {
					Thread.sleep(100); // msecs
				} catch (InterruptedException e1) {
				}
			} catch (IOException e) {
				outputRaf.close();
				throw e;
			}
		}

		try {
			inputRaf.seek(0);
			byte[] header = new byte[FILE_HEADER_SIZE];
			int bytesRead = inputRaf.read(header);
			if (bytesRead != header.length) {
				throw new IOException(
						"Error reading NEXRAD2 header -- got " + bytesRead + " rather than" + header.length);
			}

			System.out.println(new String(header));

			outputRaf.write(header);

			boolean eof = false;
			int numCompBytes;
			byte[] ubuff = new byte[40000];
			byte[] obuff = new byte[40000];

			CBZip2InputStream cbzip2 = new CBZip2InputStream();
			while (!eof) {
				try {
					numCompBytes = inputRaf.readInt();
					if (numCompBytes == -1) {
						if (log.isDebugEnabled())
							log.debug("  done: numCompBytes=-1 ");
						break;
					}
				} catch (EOFException ee) {
					//log.debug("got EOFException");
					break; // assume this is ok
				}
/*
				if (log.isDebugEnabled()) {
				log.debug("reading compressed bytes " + numCompBytes + " input starts at " + inputRaf.getFilePointer()
						+ "; output starts at " + outputRaf.getFilePointer());

				 }
				 */
				/*
				 * For some stupid reason, the last block seems to have the
				 * number of bytes negated. So, we just assume that any negative
				 * number (other than -1) is the last block and go on our merry
				 * little way.
				 */
				if (numCompBytes < 0) {
					if (log.isDebugEnabled())
					log.debug("last block?" + numCompBytes);
					numCompBytes = -numCompBytes;
					eof = true;
				}
				byte[] buf = new byte[numCompBytes];
				inputRaf.readFully(buf);
				ByteArrayInputStream bis = new ByteArrayInputStream(buf, 2, numCompBytes - 2);

				// CBZip2InputStream cbzip2 = new CBZip2InputStream(bis);
				cbzip2.setStream(bis);
				int total = 0;
				int nread;
				/*
				 * while ((nread = cbzip2.read(ubuff)) != -1) {
				 * dout2.write(ubuff, 0, nread); total += nread; }
				 */
				try {
					while ((nread = cbzip2.read(ubuff)) != -1) {
						if (total + nread > obuff.length) {
							byte[] temp = obuff;
							obuff = new byte[temp.length * 2];
							System.arraycopy(temp, 0, obuff, 0, temp.length);
						}
						System.arraycopy(ubuff, 0, obuff, total, nread);
						total += nread;
					}
					if (obuff.length >= 0)
						outputRaf.write(obuff, 0, total);
				} catch (BZip2ReadException ioe) {
					log.warn("Nexrad2IOSP.uncompress ", ioe);
				}
				float nrecords = (float) (total / 2432.0);
				//if (log.isDebugEnabled())
				//log.debug("  unpacked " + total + " num bytes " + nrecords + " records; ouput ends at "
					//	+ outputRaf.getFilePointer());
			}

			outputRaf.flush();
		} catch (IOException e) {
			if (outputRaf != null)
				outputRaf.close();

			// dont leave bad files around
			File ufile = new File(ufilename);
			if (ufile.exists()) {
				if (!ufile.delete())
					log.warn("failed to delete uncompressed file (IOException)" + ufilename);
			}

			throw e;
		} finally {
			try {
				if (lock != null)
					lock.release();
			} catch (IOException e) {
				if (outputRaf != null)
					outputRaf.close();
				throw e;
			}
		}

		return outputRaf;
	}

	/**
	 * Write equivilent uncompressed version of the file.
	 *
	 * @param inputRaf
	 *            file to uncompress
	 * @param ufilename
	 *            write to this file
	 * @return raf of uncompressed file
	 * @throws IOException
	 *             on read error
	 */
	private static RandomAccessFile uncompressChunk(RandomAccessFile inputRaf, String ufilename) throws IOException {
		RandomAccessFile outputRaf = new RandomAccessFile(ufilename, "rw");
		FileLock lock;

		while (true) { // loop waiting for the lock
			try {
				lock = outputRaf.getRandomAccessFile().getChannel().lock(0, 1, false);
				break;

			} catch (OverlappingFileLockException oe) { // not sure why lock()
														// doesnt block
				try {
					Thread.sleep(100); // msecs
				} catch (InterruptedException e1) {
				}
			} catch (IOException e) {
				outputRaf.close();
				throw e;
			}
		}

		try {
			inputRaf.seek(0);
			// byte[] header = new byte[4];
			// int bytesRead = inputRaf.read(header);
			// if (bytesRead != header.length) {
			// throw new IOException(
			// "Error reading NEXRAD2 header -- got " + bytesRead + " rather
			// than" + header.length);
			// }
			// outputRaf.write(header);

			boolean eof = false;
			int numCompBytes;
			byte[] ubuff = new byte[40000];
			byte[] obuff = new byte[40000];

			CBZip2InputStream cbzip2 = new CBZip2InputStream();
			while (!eof) {
				try {
					numCompBytes = inputRaf.readInt();
					if (numCompBytes == -1) {
						if (log.isDebugEnabled())
							log.debug("  done: numCompBytes=-1 ");
						break;
					}
				} catch (EOFException ee) {
					//log.debug("got EOFException");
					break; // assume this is ok
				}

				if (log.isDebugEnabled()) {
				//	log.debug("reading compressed bytes " + numCompBytes + " input starts at "
					//		+ inputRaf.getFilePointer() + "; output starts at " + outputRaf.getFilePointer());

				}
				/*
				 * For some stupid reason, the last block seems to have the
				 * number of bytes negated. So, we just assume that any negative
				 * number (other than -1) is the last block and go on our merry
				 * little way.
				 */
				if (numCompBytes < 0) {
					if (log.isDebugEnabled())
						log.debug("last block?" + numCompBytes);
					numCompBytes = -numCompBytes;
					eof = true;
				}
				byte[] buf = new byte[numCompBytes];
				inputRaf.readFully(buf);
				ByteArrayInputStream bis = new ByteArrayInputStream(buf, 2, numCompBytes - 2);

				// CBZip2InputStream cbzip2 = new CBZip2InputStream(bis);
				cbzip2.setStream(bis);
				int total = 0;
				int nread;
				/*
				 * while ((nread = cbzip2.read(ubuff)) != -1) {
				 * dout2.write(ubuff, 0, nread); total += nread; }
				 */
				try {
					while ((nread = cbzip2.read(ubuff)) != -1) {
						if (total + nread > obuff.length) {
							byte[] temp = obuff;
							obuff = new byte[temp.length * 2];
							System.arraycopy(temp, 0, obuff, 0, temp.length);
						}
						System.arraycopy(ubuff, 0, obuff, total, nread);
						total += nread;
					}
					if (obuff.length >= 0)
						outputRaf.write(obuff, 0, total);
				} catch (BZip2ReadException ioe) {
					log.warn("Nexrad2IOSP.uncompress ", ioe);
				}
				float nrecords = (float) (total / 2432.0);
				//if (log.isDebugEnabled())
				// log.debug("  unpacked " + total + " num bytes " + nrecords + " records; ouput ends at "
				//		+ outputRaf.getFilePointer());
			}

			outputRaf.flush();
		} catch (IOException e) {
			if (outputRaf != null)
				outputRaf.close();

			// dont leave bad files around
			File ufile = new File(ufilename);
			if (ufile.exists()) {
				if (!ufile.delete())
					log.warn("failed to delete uncompressed file (IOException)" + ufilename);
			}

			throw e;
		} finally {
			try {
				if (lock != null)
					lock.release();
			} catch (IOException e) {
				if (outputRaf != null)
					outputRaf.close();
				throw e;
			}
		}

		return outputRaf;
	}

}
