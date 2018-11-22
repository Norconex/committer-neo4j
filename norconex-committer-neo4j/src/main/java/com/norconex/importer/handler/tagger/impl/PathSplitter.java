package com.norconex.importer.handler.tagger.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.io.CachedInputStream;
import com.norconex.commons.lang.io.CachedStreamFactory;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import com.norconex.importer.doc.ImporterDocument;
import com.norconex.importer.doc.ImporterMetadata;
import com.norconex.importer.handler.ImporterHandlerException;
import com.norconex.importer.handler.splitter.AbstractDocumentSplitter;
import com.norconex.importer.handler.splitter.SplittableDocument;

public class PathSplitter extends AbstractDocumentSplitter implements IXMLConfigurable{
	
	private static final Logger LOGGER = LogManager.getLogger(PathSplitter.class);
	
	public PathSplitter (){
		
	}

	@Override
	protected List<ImporterDocument> splitApplicableDocument(SplittableDocument doc, OutputStream output,
			CachedStreamFactory streamFactory, boolean parsed) throws ImporterHandlerException {
		
	        
	        List<ImporterDocument> docs = new ArrayList<>();
	       // try {
	        if (doc.getMetadata().containsKey("splitted")) {
	        	//doc.getMetadata().remove("splitted");
	        	return docs;	
	        }
	        	
	        	StringTokenizer st = new StringTokenizer( doc.getReference(), "/");
	            StringBuilder b = new StringBuilder();
	            List<String> paths = new ArrayList<>();
	            while (st.hasMoreTokens()){
	            	String seg = st.nextToken();
	            	/*if (st.countTokens() == 1) b.append(seg);
	            	else*/ b.append(seg).append("/");
	            	
	                paths.add(b.toString());
	            }
	        	
	            if (paths.size() == 1) {
	            	
	                if (doc.getReference().equals(paths.get(0)) )
	                    return docs;
	                
	            }

	            // process "legit" child elements
	            final int pathsSize = paths.size();
	            for (int i = 0; i < pathsSize; i++) {
	            	final ImporterMetadata childMeta = new ImporterMetadata();
	                doc.getMetadata().setBoolean("splitted", true);
	                CachedInputStream content = null;
	                
	                //}
	            	
	            	final String path = paths.get(i);
	            	if (i == pathsSize-1){
	            		childMeta.load(doc.getMetadata());
	            		//content = streamFactory.newInputStream();
	            		//try{
	            			//doc.getReader().reset();
	            			//content = streamFactory.newInputStream(doc.getInput());
	            			content = (CachedInputStream) doc.getInput();
		            		doc.getMetadata().setString("type", "file");
		            		content = streamFactory.newInputStream();
	            		/*}
	            		catch (IOException e)
	            		{
	            			LOGGER.error("splitApplicableDocument() Error occured "+e.getMessage(),e);
	            		}*/
	            		
	            		
	            	}else{
	            		//childMeta.load(doc.getMetadata());
	            		content = streamFactory.newInputStream();
	            		childMeta.setString("type", "folder");
	            		childMeta.setString("collector.content-type", "text/directory");
	            		childMeta.setString("document.contentType", "text/directory");
	            		childMeta.setLong("collector.lastmodified", doc.getMetadata().getLong("collector.lastmodified"));
	            		childMeta.setBoolean("collector.is-crawl-new", true);
	            		
	            		childMeta.setString("Content-Location:",path);
	            	}
	            	
	            	final ImporterDocument childDoc = new ImporterDocument(path, content, childMeta);
	            	childMeta.setReference(path);
	                //childMeta.setEmbeddedReference(path);
	                /*if (i > 0)*/ //childMeta.setEmbeddedParentReference(doc.getReference());
	                /*if (i > 0)*/ //childMeta.setEmbeddedParentRootReference(doc.getReference());
	                docs.add(childDoc);
	            	
				}
	            /*for (String path : paths) {
	                ImporterMetadata childMeta = new ImporterMetadata();
	                doc.getMetadata().addBoolean("splitted", true);
	                childMeta.load(doc.getMetadata());
	                //String childContent = elm.outerHtml();
	                String childEmbedRef = path;
	                String childRef = path;
	                CachedInputStream content = null;
	                /*if (childContent.length() > 0) {
	                    content = streamFactory.newInputStream(childContent);
	                } else {
	                    content = streamFactory.newInputStream();
	                }
	                ImporterDocument childDoc = 
	                        new ImporterDocument(childRef, content, childMeta); 
	                childMeta.setReference(childRef);
	                childMeta.setEmbeddedReference(childEmbedRef);
	                childMeta.setEmbeddedParentReference(doc.getReference());
	                childMeta.setEmbeddedParentRootReference(doc.getReference());
	                docs.add(childDoc);
	            }
	         /*}catch (IOException e) {
	            throw new ImporterHandlerException(
	                    "Cannot parse document into a DOM-tree.", e);
	        }*/
	        return docs;
	}

	@Override
	protected void loadHandlerFromXML(XMLConfiguration xml) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void saveHandlerToXML(EnhancedXMLStreamWriter writer) throws XMLStreamException {
		// TODO Auto-generated method stub
		
	}
	
	public static void main (String[] args){
		StringTokenizer st = new StringTokenizer( "file:/data1/servers/norconex-collector-filesystem-2.8.1-SNAPSHOT/apidocs/com/norconex/collector/fs/", "/");
        StringBuilder b = new StringBuilder();
        List<String> paths = new ArrayList<>();
        while (st.hasMoreTokens()){
        	String seg = st.nextToken();
        	System.out.println(st.countTokens());
        	if (st.countTokens() == 0) b.append(seg);
        	else b.append(seg).append("/");
        	
            paths.add(b.toString());      
        }
        
        System.out.println(paths);
	}
}
