package com.ropeok.dataprocess.v2.meta;

import com.google.common.base.Preconditions;

import java.util.*;

/**
 * Step可以包含多个上级Step和下级Step和一个Component
 */
public class StepMetaV2 extends ElementMetaV2 {

    private Set<String> prevStepMetaV2 = new HashSet<>();
    private Set<String> nextStepMetaV2 = new HashSet<>();

    public Set<String> getPrevStepMetaV2() {
        return prevStepMetaV2;
    }

    public void setPrevStepMetaV2(Set<String> prevStepMetaV2) {
        this.prevStepMetaV2 = prevStepMetaV2;
    }

    public Set<String> getNextStepMetaV2() {
        return nextStepMetaV2;
    }

    public void setNextStepMetaV2(Set<String> nextStepMetaV2) {
        this.nextStepMetaV2 = nextStepMetaV2;
    }

    public void addPrevStepMetaV2(String prevStepId) {
        Preconditions.checkNotNull(prevStepId);
        this.prevStepMetaV2.add(prevStepId);
    }

    public void addNextStepMetaV2(String nextStepId) {
        Preconditions.checkNotNull(nextStepId);
        this.nextStepMetaV2.add(nextStepId);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
