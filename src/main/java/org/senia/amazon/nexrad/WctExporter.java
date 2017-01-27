package org.senia.amazon.nexrad;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import gov.noaa.ncdc.wct.WCTIospManager;
import gov.noaa.ncdc.wct.export.WCTExport;
import gov.noaa.ncdc.wct.export.WCTExportBatch;

public class WctExporter {
    private HashMap<String, String> configReplacementsMap = new HashMap<String, String>();
	private static final Logger log = LoggerFactory.getLogger(WctExporter.class);

	
	public void convert2NC(String inFile, String outFile, String configFile) throws NumberFormatException, XPathExpressionException, SAXException, IOException, ParserConfigurationException, InstantiationException {
        WCTExport exporter = new WCTExport();
        WCTExportBatch exporterBatch = new WCTExportBatch();
        WCTIospManager.registerWctIOSPs();
        exporter.setAutoRenameOutput(false);
        exporter.addDataExportListener(exporterBatch.new BatchExportListener());
        exporter.setOutputFormat(WCTExportBatch.readOutputFormat("nc"));
        log.info("ConfigFile: "+configFile);
        log.info("inFile: "+inFile);
        log.info("outFile: "+outFile);

        File infile;
        File outfile = new File(outFile);
        
        String configFile2 = WCTExportBatch.parseConfigPathAndReplacements(configFile, configReplacementsMap);
        WCTExportBatch.processConfigFile(exporter, new File(configFile2), configReplacementsMap);
	//BATCH RUN HERE
        infile = new File(inFile);
        try {
        	WCTExportBatch.doBatchExport(exporter, infile, outfile);
        }catch (Exception e) {
        	e.printStackTrace();
        }
	}
	
}
