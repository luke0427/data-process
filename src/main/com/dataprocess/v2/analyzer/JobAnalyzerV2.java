package com.ropeok.dataprocess.v2.analyzer;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.utils.Constants;
import com.ropeok.dataprocess.v2.meta.JobMetaV2;
import com.ropeok.dataprocess.v2.meta.StepMetaV2;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.DefaultAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JobAnalyzerV2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobAnalyzerV2.class);
    private final Map<String, JobMetaV2> jobMetaV2Map = new HashMap<>();

    public JobAnalyzerV2(InputStream inputStream) {
        try {
            init(inputStream);
        } catch (Exception e) {
            LOGGER.error("Job任务解析异常,{}", e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException("Job任务解析异常" + e.getLocalizedMessage());
        }
    }

    public Collection<JobMetaV2> getJobMetaV2s() {
        return jobMetaV2Map.values();
    }

    private void init(InputStream inputStream) throws Exception {
        SAXReader reader = new SAXReader();
        Document document = reader.read(inputStream);
        Element root = document.getRootElement();
        Iterator<Element> elements = root.elements().iterator();
        while(elements.hasNext()) {
            Element e = elements.next();
            JobMetaV2 jobMetaV2 = new JobMetaV2();
            System.out.println(e.attributes());
            Iterator<DefaultAttribute> attrIterator = e.attributeIterator();
            while(attrIterator.hasNext()) {
                DefaultAttribute attribute = attrIterator.next();
                jobMetaV2.setProperty(attribute.getName(), attribute.getStringValue());
            }
            jobMetaV2.initProperties();
            Iterator<Element> elementIterator = e.elements().iterator();
            Map<String, StepMetaV2> stepMetaV2Map = new HashMap<>();
            while(elementIterator.hasNext()) {
                Element childElement = elementIterator.next();
                childElement.getData();
                attrIterator = childElement.attributeIterator();
                StepMetaV2 stepMetaV2 = new StepMetaV2();
                stepMetaV2.setProperty(Constants.STEP_BODY, childElement.getTextTrim());
                while(attrIterator.hasNext()) {
                    DefaultAttribute attribute = attrIterator.next();
                    stepMetaV2.setProperty(attribute.getName(), attribute.getStringValue());
                }
                stepMetaV2.initProperties();
                stepMetaV2Map.put(stepMetaV2.getId(), stepMetaV2);
            }
            Iterator<Map.Entry<String, StepMetaV2>> entryIterator = stepMetaV2Map.entrySet().iterator();
            while(entryIterator.hasNext()) {
                Map.Entry<String, StepMetaV2> entry = entryIterator.next();
                StepMetaV2 stepMetaV2 = entry.getValue();
                //关联上一步或者下一步
                String prev = stepMetaV2.getStringProperty("prev");
                String next = stepMetaV2.getStringProperty("next");
                if(StringUtils.isNotBlank(prev)) {
                    String[] strSteps = prev.split(",");
                    for(String strStep : strSteps) {
                        StepMetaV2 prevStepMetaV2 = stepMetaV2Map.get(strStep);
                        Preconditions.checkNotNull(prevStepMetaV2);
                        stepMetaV2.addPrevStepMetaV2(prevStepMetaV2.getId());
                        prevStepMetaV2.addNextStepMetaV2(stepMetaV2.getId());
                    }
                }
                if(StringUtils.isNotBlank(next)) {
                    String[] strSteps = next.split(",");
                    for(String strStep : strSteps) {
                        StepMetaV2 nextStepMetaV2 = new StepMetaV2();
                        if(strStep.contains(":")) {
                            nextStepMetaV2 = stepMetaV2Map.get(strStep.substring(strStep.indexOf(":")+1));
                        } else {
                            nextStepMetaV2 = stepMetaV2Map.get(strStep);
                        }
                        Preconditions.checkNotNull(nextStepMetaV2);
                        stepMetaV2.addNextStepMetaV2(nextStepMetaV2.getId());
                        nextStepMetaV2.addPrevStepMetaV2(stepMetaV2.getId());
                    }
                }
            }
            jobMetaV2.setStepMetaV2Map(stepMetaV2Map);

            jobMetaV2Map.put(jobMetaV2.getKey(), jobMetaV2);
        }
    }
}
