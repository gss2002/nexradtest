package org.senia.amazon.nexrad;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

public class AwsS3 {
	private static final Logger log = LoggerFactory.getLogger(AwsS3.class);

	public File downloadNexrad(String path, String outputPath) throws IOException {
		File file = null;
		/*
		 * Get AWS Anonymous Creds for NexradL2 Bucket
		 */
		try {
			AWSCredentials creds = new AnonymousAWSCredentials();

			Region eastRegion = Region.getRegion(Regions.US_EAST_1);
			String nexradL2Bucket = "unidata-nexrad-level2-chunks";

			AmazonS3 s3 = new AmazonS3Client(creds);
			s3.setRegion(eastRegion);

			ListObjectsRequest lor = new ListObjectsRequest().withBucketName(nexradL2Bucket).withPrefix(path);
			ObjectListing objectListing = s3.listObjects(lor);
			log.debug("Nexrad Amazon Bucket: " + objectListing.getBucketName());
			int lastObject = objectListing.getObjectSummaries().size();
			log.debug("Number of Available Nexrad Objects: " + String.valueOf(lastObject));
			String radarObj = objectListing.getObjectSummaries().get(lastObject - 1).getKey();
			for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
				log.debug(summary.getKey());
			}
			S3Object obj = s3.getObject(new GetObjectRequest(nexradL2Bucket, radarObj));
			file = new File(outputPath+"/" + radarObj);

			// if the directory does not exist, create it
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}

			IOUtils.copy(obj.getObjectContent(), new FileOutputStream(file));
			log.debug(radarObj);
		} catch (Exception ue) {
			ue.printStackTrace();
		}
		return file;

	}
}
