/**
 * 严肃声明：
 * 开源版本请务必保留此注释头信息，若删除我方将保留所有法律责任追究！
 * 本系统已申请软件著作权，受国家版权局知识产权以及国家计算机软件著作权保护！
 * 可正常分享和学习源码，不得用于违法犯罪活动，违者必究！
 * Copyright (c) 2019-2020 十三 all rights reserved.
 * 版权所有，侵权必究！
 */
package ltd.newbee.mall;

import ltd.newbee.mall.common.Constants;
import ltd.newbee.mall.common.NewBeeMallException;
import ltd.newbee.mall.common.ServiceResultEnum;
import ltd.newbee.mall.dao.NewBeeMallOrderMapper;
import ltd.newbee.mall.entity.NewBeeMallOrder;
import ltd.newbee.mall.redis.RedisCache;
import ltd.newbee.mall.service.AlipayRefundRecordService;
import ltd.newbee.mall.service.NewBeeMallOrderService;
import ltd.newbee.mall.util.MD5Util;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@MapperScan("ltd.newbee.mall.dao")
@SpringBootApplication
public class NewBeeMallPlusApplication implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(NewBeeMallPlusApplication.class, args);
    }

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private NewBeeMallOrderService newBeeMallOrderService;

    @Autowired
    private AlipayRefundRecordService alipayRefundRecordService;

    //服务启动后自动调用的方法
    @Override
    @Transactional
    public void run(String... args) throws Exception {
        //将redis的订单主键和mysql里的订单主键保持一直
        Long newOrderCount = newBeeMallOrderService.selectMaxOrderId();
        redisCache.setCacheObject(Constants.ORDER_ID_COUNT, newOrderCount);
        //启动springboot完毕后进行redis、数据库过期数据同步
        List<NewBeeMallOrder> orders = newBeeMallOrderService.selectAllOrderIsPayedAndUnpay();

        List<Long> orderIds = new ArrayList<>();
        Date d = new Date();
        Calendar c = Calendar.getInstance();
        for(NewBeeMallOrder order : orders){
            c.setTime(order.getCreateTime());
            c.add(Calendar.MINUTE, 30);
            if(c.getTime().getTime() < d.getTime()){
                //已支付
                if(order.getOrderStatus() == 1){
                    if(alipayRefundRecordService.selectByOrderNo(order.getOrderNo()) == null){
                        //超时退款标记
                        newBeeMallOrderService.refund(order.getOrderNo(), null);
                    }
                    orderIds.add(order.getOrderId());
                    redisCache.deleteObject(getPayKey(order));
                }
                //未支付
                else if(order.getOrderStatus() == 0){
                    orderIds.add(order.getOrderId());
                    redisCache.deleteObject(getPayKey(order));
                }
            }
        }
        try{
            //只有存在超时订单的情况才进行处理
            if(orderIds != null && orderIds.size() != 0){
                int row = newBeeMallOrderService.timeOutClose(orderIds, d);
                if(row < 1) NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
            }
        }catch (Exception e){
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }
    }

    private String getPayKey(NewBeeMallOrder order) {
        StringBuffer orderKey = new StringBuffer();
        orderKey.append(Constants.PAY + ".");
        String userId = order.getUserId() + ".";
        String orderNo = order.getOrderNo() + ".";
        orderKey.append(userId);
        orderKey.append(orderNo);
        orderKey.append(MD5Util.getSuffix());
        return orderKey.toString();
    }
}
