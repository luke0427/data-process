package com.ropeok.dataprocess.utils;

import com.google.common.base.Preconditions;
import com.ropeok.dataprocess.meta.ColumnMeta;
import com.ropeok.dataprocess.handler.ProcHandler;
import com.ropeok.dataprocess.meta.ProcHandlerMeta;
import com.ropeok.dataprocess.handler.ProcHandlerType;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class ProcHandlerUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcHandlerUtils.class);

    public static ProcHandler initProcHandlerChain(ProcHandlerMeta procHandlerMeta) throws IllegalAccessException, InstantiationException {
        Class<?> clazz = ProcHandlerType.getProcHandler(procHandlerMeta.getProcHandlerType().name());
        ProcHandler procHandler = (ProcHandler) clazz.newInstance();
        procHandler.setProcHandlerMeta(procHandlerMeta);
        procHandler.init();
        if(procHandlerMeta.getNextProcHandlerMeta() != null) {
            procHandler.setNextHandler(initProcHandlerChain(procHandlerMeta.getNextProcHandlerMeta()));
        }
        return procHandler;
    }

    public static ProcHandlerMeta initHandlers(Element element) {
        List<Element> list = element.elements();
        List<ProcHandlerMeta> procHandlerMetas = new LinkedList<>();
        for(int i= 0; i < list.size(); i++) {
            Element e = list.get(i);
//            LOGGER.info("Handle: {}", e);
            ProcHandlerMeta procHandlerMeta = new ProcHandlerMeta();
            String procHandlerType = e.attributeValue("type");
            Preconditions.checkNotNull(procHandlerType);
            procHandlerMeta.setProcHandlerType(ProcHandlerType.getProcHandlerType(procHandlerType));
            Preconditions.checkNotNull(procHandlerMeta.getProcHandlerType());
            //继续初始化ProcHandlerMeta的其他属性
            initAttributes(e, procHandlerMeta);
            procHandlerMetas.add(procHandlerMeta);

            if(i > 0) {
                procHandlerMetas.get(i-1).setNextProcHandlerMeta(procHandlerMeta);
            }
        }

        return procHandlerMetas.get(0);
    }

    public static void initAttributes(Element element, ProcHandlerMeta procHandlerMeta) {
        procHandlerMeta.setFormat(element.attributeValue("format"));
        procHandlerMeta.setIgnore(element.attributeValue("ignore"));
        procHandlerMeta.setTocolumn(element.attributeValue("tocolumn"));
        procHandlerMeta.setUrl(element.attributeValue("url"));
        procHandlerMeta.setUsername(element.attributeValue("username"));
        procHandlerMeta.setPassword(element.attributeValue("password"));
        procHandlerMeta.setKeyColumn(element.attributeValue("keyColumn"));
        procHandlerMeta.setCacheKeyCol(element.attributeValue("cacheKeyCol"));
        procHandlerMeta.setSql(element.getTextTrim());//sql in text
        procHandlerMeta.setMultiValue(element.attributeValue("multiValue"));
        procHandlerMeta.setPickBirth(element.attributeValue("pickBirth"));
        procHandlerMeta.setBirthColumn(element.attributeValue("birthColumn"));
        procHandlerMeta.setPickType(element.attributeValue("pickType"));
        procHandlerMeta.setTypeColumn(element.attributeValue("typeColumn"));
        procHandlerMeta.setPickSex(element.attributeValue("pickSex"));
        procHandlerMeta.setSexColumn(element.attributeValue("sexColumn"));
        procHandlerMeta.setColumn(element.attributeValue("column"));
        procHandlerMeta.setServerip(element.attributeValue("serverip"));
        procHandlerMeta.setServerport(StringUtils.isNotBlank(element.attributeValue("serverport")) ? Integer.parseInt(element.attributeValue("serverport")) : 0);
        procHandlerMeta.setKeyPrefix(element.attributeValue("keyPrefix"));

        String retain = element.attributeValue("retain");
        if(StringUtils.isNotBlank(retain)) {
            procHandlerMeta.setParam("retain", retain);
        }

        List<ColumnMeta> columnMetas = new LinkedList<>();

        String columns = StringUtils.isNotBlank(element.attributeValue("columns")) ? element.attributeValue("columns") : element.attributeValue("cacheColumns");
        if(StringUtils.isNotBlank(columns)) {
            String[] columnArray = columns.split(",");
            String tocolumns = element.attributeValue("tocolumns");
            String[] tocolumnArray = null;
            if(StringUtils.isNotBlank(tocolumns)) {
                tocolumnArray = tocolumns.split(",");
                Preconditions.checkArgument(tocolumnArray.length == columnArray.length);
            }
            int i = 0;
            for(String col : columnArray) {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setColumnName(col.trim());
                columnMetas.add(columnMeta);
                if(tocolumnArray != null) {
                    columnMeta.setToName(tocolumnArray[i].trim());
                }
                i++;
            }
        }
        if(columnMetas.size() > 0) {
            procHandlerMeta.setColumnMetas(columnMetas);
        }
        String strValues = element.attributeValue("values");
        if(StringUtils.isNotBlank(strValues)) {
            String[] valueArr = strValues.split(",");
            List<Object> values = new LinkedList<>();
            for (Object value : valueArr) {
                values.add(value);
            }
            if(values.size() > 0) {
                procHandlerMeta.setValues(values);
            }
        }
    }

    /*public static ProcHandler initProcHandlerChain(JSONObject element, ProcHandler prev) {
        String type = element.getString("type");
        //获取类型，初始化相应类型，如果有下一个handler就递归直到handler为null;
        ProcHandler ph = null;
        switch(type) {
            case NotNullProcHandler.PROC_HANDLER_TYPE:
                NotNullProcHandler notNullProcHandler = new NotNullProcHandler();
                notNullProcHandler.setRowMeta(element.getObject("rowMeta", RowMeta.class));
                ph = notNullProcHandler;
                break;
            case SfzhValidProcHandler.PROC_HANDLER_TYPE:
                SfzhValidProcHandler sfzhValidProcHandler = new SfzhValidProcHandler();
                sfzhValidProcHandler.setRowMeta(element.getObject("rowMeta", RowMeta.class));
                ph = sfzhValidProcHandler;
                break;
            case ColumnMappingProcHandler.PROC_HANDLER_TYPE:
                ColumnMappingProcHandler columnMappingProcHandler = new ColumnMappingProcHandler();
                columnMappingProcHandler.setRowMeta(element.getObject("rowMeta", RowMeta.class));
                ph = columnMappingProcHandler;
                break;
            case PKMD5ProcHandler.PROC_HANDLER_TYPE:
                PKMD5ProcHandler pkmd5ProcHandler = new PKMD5ProcHandler();
                pkmd5ProcHandler.setRowMeta(element.getObject("rowMeta", RowMeta.class));
                ph = pkmd5ProcHandler;
                break;
            default:
                break;
        }
        JSONObject nextHandler = element.getJSONObject("nextHandler");
        if (nextHandler != null) {
            ph.setNextHandler(initProcHandlerChain(nextHandler, ph));
        }

        return ph;
    }*/

}
