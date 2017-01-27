package org.senia.amazon.nexrad;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NexradPropReader {	
	private static final Logger log = LoggerFactory.getLogger(NexradPropReader.class);

    public Properties getNexradConfig(String propPath) throws IOException {
        InputStream is = null;
        try {
                URL url = new URL(propPath);
                URLConnection conn = url.openConnection();
                is = conn.getInputStream();
        } catch (IOException ioe) {
                log.debug("IO Exception " + ioe );
        }
        Properties prop = new Properties();
        prop.load(is);
        return prop;

   }
}
