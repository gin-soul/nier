package cn.nier.weblog.service;

import cn.nier.weblog.mapper.FlowNumMapper;
import cn.nier.weblog.pojo.FlowNumPojo;
import cn.nier.weblog.pojo.FlowReturnPojo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@Transactional
public class FlowNumServiceImpl implements FlowNumService {
    @Autowired
    private FlowNumMapper flowNumMapper;
    @Override
    public FlowReturnPojo getAllFlowNum() {
        FlowReturnPojo flowReturnPojo = new FlowReturnPojo();
        ArrayList<String> dates = new ArrayList<String>();
        ArrayList<String> uvs = new ArrayList<String>();
        ArrayList<String> new_uvs = new ArrayList<String>();

        List<FlowNumPojo> flowNumList = flowNumMapper.getAllFlowNum();
        for (FlowNumPojo flowNumPojo : flowNumList) {
            dates.add(flowNumPojo.getDateStr());
            uvs.add(flowNumPojo.getNewUvNum());
            new_uvs.add(flowNumPojo.getNewUvNum());
        }
        flowReturnPojo.setDates(dates);
        flowReturnPojo.setNew_uvs(new_uvs);
        System.out.println(flowReturnPojo);
        return flowReturnPojo;
    }
}












