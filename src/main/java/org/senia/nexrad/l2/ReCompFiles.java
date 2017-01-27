package org.senia.nexrad.l2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import ucar.unidata.io.RandomAccessFile;
import ucar.unidata.io.bzip2.BZip2ReadException;
import ucar.unidata.io.bzip2.CBZip2InputStream;
import ucar.unidata.io.bzip2.CBZip2OutputStream;


public class ReCompFiles {
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ReCompFiles.class);
	public static final int FILE_HEADER_SIZE = 24;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File file = new File(args[0]);
		String outputdir = args[1];
		RandomAccessFile raf = null;
		Collection<File> files = FileUtils.listFiles(file, null, true);	
		for (File file2 : files) {
			try {
				raf = ucar.unidata.io.RandomAccessFile.acquire(file2.getPath());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (file2.getName().endsWith("001-S")) {
				try {
					ReCompFiles.compress(raf, outputdir+"/"+file2.getName());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					ReCompFiles.compressChunk(raf, outputdir+"/"+file2.getName());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}
	}

	/**
	 * Write equivilent compressed version of the file.
	 *
	 * @param inputRaf
	 *            file to be compressed
	 * @param ufilename
	 *            write to this file
	 * @throws IOException
	 *             on read error
	 */
	public static void compressChunk(RandomAccessFile inputRaf, String ufilename) throws IOException {
		byte[] dataFile = new byte[(int)inputRaf.length()-6];
		System.out.println(inputRaf.length());
		DataInputStream dataIs = new DataInputStream(new FileInputStream(inputRaf.getLocation()));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();				
		byte[] buffer = new byte[1024];
		int read = 0;
		dataIs.skipBytes(6);
		while ((read = dataIs.read(buffer, 0, buffer.length)) != -1) {
			baos.write(buffer, 0, read);
		}		
		baos.flush();		
		dataFile = baos.toByteArray();
		File fileToWriteTo = new File(ufilename);
	    FileUtils.writeByteArrayToFile(fileToWriteTo, dataFile);
	    dataIs.close();
	    
	   
	}
	
	/**
	 * Write equivilent uncompressed version of the file.
	 *
	 * @param inputRaf
	 *            file to uncompress
	 * @param ufilename
	 *            write to this file
	 * @throws IOException
	 *             on read error
	 */
	private static void compress(RandomAccessFile inputRaf, String ufilename) throws IOException {
		byte[] dataFile = new byte[(int)inputRaf.length()];
		DataInputStream dataIs = new DataInputStream(new FileInputStream(inputRaf.getLocation()));
		dataIs.readFully(dataFile);
	    File fileToWriteTo = new File(ufilename);
	    
	    FileUtils.writeByteArrayToFile(fileToWriteTo, dataFile);
	    dataIs.close();
	}
	
}
