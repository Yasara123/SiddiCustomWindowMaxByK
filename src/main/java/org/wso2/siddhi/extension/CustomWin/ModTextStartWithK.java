package org.wso2.siddhi.extension.CustomWin;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
/*
* #Customwindow.ModText('Tweet text','@','RT'....)
* Sample Query:
* from inputStream#Customwindow.ModText('Tweet text','RT',':'...)
* select attribute1, attribute2
* insert into outputStream;
* 
*  RT @Ready4Martin: @MartinOMalley Receives Support From Obama’s Finance ...
* */

public class ModTextStartWithK extends StreamProcessor{
	private static int NoOfParam;
	String sapertator[];
	VariableExpressionExecutor variableExpressionExecutor;
   
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object[] currentState() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void restoreState(Object[] state) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void process(ComplexEventChunk<StreamEvent> streamEventChunk,
			Processor nextProcessor, StreamEventCloner streamEventCloner,
			ComplexEventPopulater complexEventPopulater) {
        int i;
        String tem = null;
        StreamEvent outData = null;
		// TODO Auto-generated method stub
		ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
			        StreamEvent streamEvent = streamEventChunk.getFirst();
	        while (streamEventChunk.hasNext()) { 
	        	tem=(String) variableExpressionExecutor.execute(streamEvent);
	        	//tem = tem.replaceAll( "\\s*\\bRT\\b\\s*", "");
	        	boolean found = false;
	        	Pattern p = Pattern.compile("[A-Za-z]");
	        	Matcher m = p.matcher(tem); 
	        	
	        	for(int j=0;j<NoOfParam;j++){
	        		if(!m.find()) { 	
	        			tem = tem.replaceAll("\\s*[\\"+sapertator[j]+"]\\s*", "");	 
	        		}else{
	        			tem = tem.replaceAll( "\\s*\\b"+sapertator[j]+"\\b\\s*", "");
	        		}
	        	}
	        	tem = tem.replaceAll((tem.split(" ")[0]), "");	        	
	        	//streamEvent.setOutputData(tem, variableExpressionExecutor.getPosition()[2]);
	        	switch (variableExpressionExecutor.getPosition()[2]) {
    	        case 0:streamEvent.setBeforeWindowData(tem, variableExpressionExecutor.getPosition()[3]);
    	        	break;
    	        case 1: streamEvent.setOnAfterWindowData(tem, variableExpressionExecutor.getPosition()[3]);
	        		break;
    	        case 2:streamEvent.setOutputData(tem, variableExpressionExecutor.getPosition()[3]);
    	        	break;	    	        				    	        
	        }	     	
	        	returnEventChunk.add(streamEvent);	
	    	    nextProcessor.process(returnEventChunk);	        	
	            streamEvent = streamEvent.getNext();
	        }	
	}

	@Override
	protected List<Attribute> init(AbstractDefinition inputDefinition,
			ExpressionExecutor[] attributeExpressionExecutors,
			ExecutionPlanContext executionPlanContext) {
		// TODO Auto-generated method stub
		  if (attributeExpressionExecutors[0]  instanceof VariableExpressionExecutor){
			  variableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];			  
		  }else {
	            throw new UnsupportedOperationException("The first parameter should be an integer");
	        }
		  NoOfParam=attributeExpressionLength-1;
		  for(int i=0;i<attributeExpressionLength-1;i++){
		  if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor){
			  sapertator[i]=((String)attributeExpressionExecutors[1].execute(null));			  
		  }else {
	            throw new UnsupportedOperationException("The first parameter should be an integer");
	        }
		  }
		  
		return null;
	}

}

