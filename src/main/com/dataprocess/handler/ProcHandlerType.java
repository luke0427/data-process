package com.ropeok.dataprocess.handler;

import com.ropeok.dataprocess.handler.impl.*;

public enum ProcHandlerType {

    NotNull(NotNullProcHandler.class),
    SfzhValid(SfzhValidProcHandler.class),
    ColumnMapping(ColumnMappingProcHandler.class),
    PKMD5(PKMD5ProcHandler.class),
    JDBCCache(JDBCCacheProcHandler.class),
    Redis(RedisProcHandler.class),
    Trim(TrimProcHandler.class),
    ValueFilter(ValueFilterProcHandler.class),
    LonLat(LonLatProcHandler.class),
    RemoveColumn(RemoveColumnProcHandler.class),
    DateFormat(DateFormatProcHandler.class),
    RemoveBlank(RemoveBlankProcHandler.class),
    AddColumn(AddColumnProcHandler.class),
    CommunityAddrMatch(CommunityAddrMatchProcHandler.class),
    PrintData(PrintDataProcHandler.class),
    PersonId(PersonIdProcHandler.class);

    private Class<? extends ProcHandler> clazz;

    ProcHandlerType(Class<? extends ProcHandler> clazz) {
        this.clazz = clazz;
    }

    public static ProcHandlerType getProcHandlerType(String name) {
        for(ProcHandlerType ht : values()) {
            if(ht.name().equals(name)) {
                return ht;
            }
        }
        return null;
    }

    public static Class<? extends ProcHandler> getProcHandler(String name) {
        for(ProcHandlerType ht : values()) {
            if(ht.name().equals(name)) {
                return ht.clazz;
            }
        }
        return null;
    }
}
