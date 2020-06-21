package com.ropeok.dataprocess.v2.meta;

import java.util.Map;

/**
 * Job 配置信息，Job包含step组成的集合
 */
public class JobMetaV2 extends ElementMetaV2 {

    private String group;
    private String desc;
    private String cron;
    private Map<String, StepMetaV2> stepMetaV2Map;

    public void initProperties() {
        super.initProperties();
        this.group = getStringProperty("group");
        this.cron = getStringProperty("cron");
        this.desc = getStringProperty("desc");
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
        setProperty("group", group);
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
        setProperty("desc", desc);
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
        setProperty("cron", cron);
    }

    public Map<String, StepMetaV2> getStepMetaV2Map() {
        return stepMetaV2Map;
    }

    public void setStepMetaV2Map(Map<String, StepMetaV2> stepMetaV2Map) {
        this.stepMetaV2Map = stepMetaV2Map;
        setProperty("stepMetaV2Map", stepMetaV2Map);
    }

    public String getKey() {
        return group + "." + getName();
    }
}
