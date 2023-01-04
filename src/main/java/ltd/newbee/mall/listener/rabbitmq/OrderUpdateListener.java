package ltd.newbee.mall.listener.rabbitmq;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.Channel;
import ltd.newbee.mall.common.*;
import ltd.newbee.mall.dao.NewBeeMallOrderMapper;
import ltd.newbee.mall.entity.NewBeeMallOrder;
import ltd.newbee.mall.rabbitmq.RabbitmqConstant;
import ltd.newbee.mall.redis.RedisCache;
import ltd.newbee.mall.service.AlipayPayRecordService;
import ltd.newbee.mall.service.NewBeeMallOrderService;
import ltd.newbee.mall.util.MD5Util;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
@RabbitListener(queues = RabbitmqConstant.ORDER_UPDATE_QUEUE)
public class OrderUpdateListener {
    private static Logger logger = LoggerFactory.getLogger(OrderUpdateListener.class);

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private NewBeeMallOrderMapper newBeeMallOrderMapper;

    @Autowired
    private NewBeeMallOrderService newBeeMallOrderService;

    @Autowired
    private AlipayPayRecordService alipayPayRecordService;

    @RabbitHandler
    public void receiveUpdateOrder(String orderInJson, Message message, Channel channel) throws IOException {
        NewBeeMallOrder order = JSON.parseObject(orderInJson, NewBeeMallOrder.class);
        NewBeeMallOrder o = null;
        try{
            //判断订单信息是否存在
            o = newBeeMallOrderMapper.selectByOrderNo(order.getOrderNo());
            if(o != null){
                //判断数据库中的订单状态是否已经为已支付
                if (o.getOrderStatus() == NewBeeMallOrderStatusEnum.ORDER_PAID.getOrderStatus()
                        || o.getPayStatus() == PayStatusEnum.PAY_SUCCESS.getPayStatus()) {
                    NewBeeMallException.fail("订单状态已为已支付状态");
                }
            }else{
                throw new NewBeeMallException("订单信息不存在");
            }
            if(!newBeeMallOrderService.updateByPrimaryKeySelective(order)) NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
            //未支付状态转为已支付状态成功时，将付款记录表中的状态改为已付款
            if(alipayPayRecordService.selectByOrderNo(order.getOrderNo()) != null){
                if(alipayPayRecordService.updateStatus(Constants.ALIPAY_STATUS_PAYED, order.getOrderNo()) < 1){
                    NewBeeMallException.fail("付款记录状态更新失败");
                }
            }else{
                NewBeeMallException.fail("付款记录不存在");
            }
            logger.info("订单号为" + order.getOrderNo() + "的订单由未支付状态转为已支付状态");
        }catch (Exception e){
            e.printStackTrace();
            logger.error("订单号为" + order.getOrderNo() + "的订单转换支付状态失败，原因：" + e.getMessage());
            StringBuffer key = new StringBuffer();
            key.append(Constants.PAY + ".");
            key.append(order.getUserId() + ".");
            key.append(order.getOrderNo() + ".");
            key.append(MD5Util.getSuffix());
            try{
                //未支付转已支付出现异常，判断订单信息是否存在，存在则将信息转为未支付，超时时间沿用之前的
                if(redisCache.isExist(key.toString())){
                    if(o != null){
                        Long expire = redisCache.getExpire(key.toString(), TimeUnit.SECONDS);
                        redisCache.setCacheObject(key.toString(), JSON.toJSONString(o), expire, TimeUnit.SECONDS);
                    }else{
                        NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
                    }
                }
            }catch (Exception ex){
                NewBeeMallException.fail(ServiceResultEnum.REDIS_ERROR.getResult());
            }
            //进行退款标记
            if(order.getOrderStatus() == 1){
                //若为已支付订单则进行退款标记
                newBeeMallOrderService.refund(order.getOrderNo(), null);
            }
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }
    }
}
