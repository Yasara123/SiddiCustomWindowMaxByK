package org.wso2.siddhi.extension.CustomWin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
/*
* #Customwindow.ModText('Tweet text','@','RT'....)
* Sample Query:
* from inputStream#Customwindow.ModText('Tweet text','RT',':'...)
* select attribute1, attribute2
* insert into outputStream;
* 
*  RT @Ready4Martin: @MartinOMalley: @MartinOMalley Receives Support From Obama’s Finance ...
*  => Receives Support From Obama’s Finance ...
* */
public class ModText extends FunctionExecutor{
	private static int NoOfParam;
	String sapertator[];
	VariableExpressionExecutor variableExpressionExecutor;
	
   
	@Override
	public Type getReturnType() {
		// TODO Auto-generated method stub
		return Type.STRING;
	}

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
	protected void init(ExpressionExecutor[] attributeExpressionExecutors,
			ExecutionPlanContext executionPlanContext) {
		// TODO Auto-generated method stub
		  if (attributeExpressionExecutors[0]  instanceof VariableExpressionExecutor){
			  variableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];			  
		  }else {
	            throw new UnsupportedOperationException("The first parameter should be an String");
	        }
		  NoOfParam=attributeExpressionExecutors.length-1;
		  for(int i=0;i<NoOfParam;i++){
		  if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor){
			  sapertator[i]=((String)attributeExpressionExecutors[1].execute(null));			  
		  }else {
	            throw new UnsupportedOperationException("The parameter should be an saperator String");
	        }
		  }
		  
		
	}

	@Override
	protected Object execute(Object[] data) {
		// TODO Auto-generated method stub
		if (data[0] == null) {
			throw new ExecutionPlanRuntimeException("Invalid input given to str:replaceAll() function. First argument cannot be null");
			}
			if (data[1] == null) {
			throw new ExecutionPlanRuntimeException("Invalid input given to str:replaceAll() function. Second argument cannot be null");
			}
        String source = (String) data[0];
    	Pattern p = Pattern.compile("[A-Za-z]");
    	Matcher m = p.matcher(source);     	
    	for(int j=0;j<NoOfParam;j++){
    		if(!m.find()) { 	
    			source  = source.replaceAll("\\s*[\\"+sapertator[j]+"]\\s*", "");	 
    		}else{
    			source = source.replaceAll( "\\s*\\b"+sapertator[j]+"\\b\\s*", "");
    		}
    		
    	}
    	//source = source.replaceAll("https?://\\S+\\s?", "");
    	source = source.replaceAll((source.split(" ")[0]), "");	
		return source;
	}

	@Override
	protected Object execute(Object data) {
		// TODO Auto-generated method stub
		return null;
	}

}
