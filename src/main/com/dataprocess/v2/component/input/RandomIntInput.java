package com.ropeok.dataprocess.v2.component.input;

import com.ropeok.dataprocess.v2.component.AbstractComponent;
import com.ropeok.dataprocess.v2.core.IDataSet;
import com.ropeok.dataprocess.v2.core.SendEvent;
import com.ropeok.dataprocess.v2.core.impl.DataSet;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RandomIntInput extends AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomIntInput.class);

    @Override
    public void init() throws Exception {
        LOGGER.info("开始初始化[{}]", getName());
    }

    @Override
    public void exec(SendEvent event) throws Exception {
        //如果有前置输入
        if(event != null) {
            //读取数据
            System.out.println(event);
            Collection<Map<String, Object>> datas = event.getDataSet().getData();
            for(Map<String, Object> data : datas) {
                LOGGER.info("接收到:{}", data);
                String num = (String) data.get("NUMBER");
                /*if(num < 1644901) {
                    throw new RuntimeException("数字太小,"+num);
                }*/
                data.put("NUMBER", "<" + getId()+ ">" + num);
                send(event);
            }
        } else {
            for(int i = 0; i < 3;i++) {
                Thread.sleep(RandomUtils.nextInt(300, 3000));
                IDataSet datas = new DataSet();
                Map<String, Object> data = new HashMap<>();
                data.put("NUMBER", getName()+RandomUtils.nextInt());
                datas.addData(data);
                LOGGER.info("输入：{}", data);
                send(new SendEvent(datas));
                stats.increRow();
            }
        }
        //TODO: 要测试一下组件内发生异常的处理(已完成)
        /*if(step.hasNextStep()) {
            step.sendToNext(this, new SendEvent(SendEvent.EventStatus.FINISHED));
        }*/
    }

}
