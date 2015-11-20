package org.wso2.siddhi.extension.CustomWin;
import java.util.*;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.executor.math.Subtract.SubtractExpressionExecutorDouble;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

/*
 * #Customwindow.maxByK('K', "ln(R)-kt", "*",'Tweet text','rank')
 * Sample Query:
 * from inputStream#Customwindow.maxByK(200,5, "ln(R)-kt", "*",'Tweet text','rank')
 * select attribute1, attribute2
 * insert into outputStream;
 * */

public class maxByK extends StreamProcessor{
	private static SubtractExpressionExecutorDouble  constantFunctionExecutor;
    private int PassToOut;
    private int Lengthtokeep;
    private ArrayList<StreamEvent> sortedWindow = new ArrayList<StreamEvent>();
    private EventComparator eventComparator;
    VariableExpressionExecutor variableExpressionExecutor;
    VariableExpressionExecutor variableExpressionCount;
    VariableExpressionExecutor variableExpressionRank;

    private class EventComparator implements Comparator<StreamEvent> {
        @Override
        public int compare(StreamEvent e1, StreamEvent e2) {
            int comparisonResult;
            int[] variablePosition = ((VariableExpressionExecutor) variableExpressionRank).getPosition();
            Comparable comparableVariable1 = (Comparable) e1.getAttribute(variablePosition);
            Comparable comparableVariable2 = (Comparable) e2.getAttribute(variablePosition);
            comparisonResult = comparableVariable1.compareTo(comparableVariable2);
            if (comparisonResult != 0) {
                return (-1) * comparisonResult;
            } else
                return 0;
        }
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
		return new Object[]{sortedWindow};
	}

	@Override
	public void restoreState(Object[] state) {
		// TODO Auto-generated method stub
		sortedWindow = (ArrayList<StreamEvent>) state[0];
	}

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        // TODO Auto-generated method stub
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        // boolean firstEle=false;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            
            for (int i = 0; i < sortedWindow.size(); i++) {
                setAttributeRank(sortedWindow.get(i), (Double) constantFunctionExecutor.execute(sortedWindow.get(i)));
            }
            setAttributeRank(streamEvent, (Double) constantFunctionExecutor.execute(streamEvent));
            boolean duplicate = false;

            if ((sortedWindow.size() <= Lengthtokeep)) {                
                duplicate = IsDuplicate(streamEvent);
                // if((duplicate==false)&&(firstEle==false)){
                if (duplicate == false) {
                    sortedWindow.add(streamEvent);
                    Collections.sort(sortedWindow, eventComparator);
                }
            } else if (((Double) variableExpressionRank.execute(sortedWindow.get(sortedWindow.size() - 1)) > (Double) variableExpressionRank
                    .execute(streamEvent))) {
                continue;
            } else {
                duplicate = IsDuplicate(streamEvent);
                // if((duplicate==false)&&(firstEle==false)){
                if (duplicate == false) {
                    sortedWindow.remove(sortedWindow.size() - 1);
                    sortedWindow.add(streamEvent);
                    Collections.sort(sortedWindow, eventComparator);

                    if (sortedWindow.size() >= Lengthtokeep) {
                        for (int j = 0; j < PassToOut; j++) {                       
                            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(sortedWindow.get(j));
                            complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { j + 1 });
                            returnEventChunk.add(clonedEvent);
                        }
                        nextProcessor.process(returnEventChunk);
                    }

                }
            }
        }

    }
	
    private void setAttributeRank(StreamEvent event, double val) {
        switch (variableExpressionRank.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionRank.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionRank.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionRank.getPosition()[3]);
        }
    }

    private boolean IsDuplicate(StreamEvent event) {
        boolean duplicate = false;
        for (int i = sortedWindow.size() - 1; i >= 0; i--) {
            if (variableExpressionExecutor.execute(sortedWindow.get(i)).equals(
                    variableExpressionExecutor.execute(event))) {
                if ((Double) variableExpressionRank.execute(sortedWindow.get(i)) < (Double) variableExpressionRank.execute(event)) {
                    setAttributeRank(sortedWindow.get(i), (Double) variableExpressionRank.execute(event));
                }
                duplicate = true;
                break;
            }
        }
        return duplicate;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            Lengthtokeep = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            PassToOut = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (!(attributeExpressionExecutors[2] instanceof SubtractExpressionExecutorDouble)) {
            throw new UnsupportedOperationException("Required a fsubstract unction, but found a other parameter");
        } else {
            constantFunctionExecutor = (SubtractExpressionExecutorDouble) attributeExpressionExecutors[2];
        }
        if (!(attributeExpressionExecutors[3] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a string parameter");
        } else {
            variableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[3];
        }
        if (!(attributeExpressionExecutors[4] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionRank = (VariableExpressionExecutor) attributeExpressionExecutors[4];
        }
        if (!(attributeExpressionExecutors[5] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionCount = (VariableExpressionExecutor) attributeExpressionExecutors[5];
        }
        eventComparator = new EventComparator();
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Index", Attribute.Type.INT));
        return attributeList;
    }

}
