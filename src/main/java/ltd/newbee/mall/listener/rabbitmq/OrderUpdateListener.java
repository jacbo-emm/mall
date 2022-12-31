package ltd.newbee.mall.listener.rabbitmq;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.Channel;
import ltd.newbee.mall.common.Constants;
import ltd.newbee.mall.common.NewBeeMallException;
import ltd.newbee.mall.common.ServiceResultEnum;
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

@Component
@RabbitListener(queues = RabbitmqConstant.ORDER_UPDATE_QUEUE)
public class OrderUpdateListener {
    private static Logger logger = LoggerFactory.getLogger(OrderUpdateListener.class);

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private NewBeeMallOrderService newBeeMallOrderService;

    @Autowired
    private AlipayPayRecordService alipayPayRecordService;

    @RabbitHandler
    public void receiveUpdateOrder(String orderInJson, Message message, Channel channel) throws IOException {
        NewBeeMallOrder order = JSON.parseObject(orderInJson, NewBeeMallOrder.class);
        try{
            if(!newBeeMallOrderService.updateByPrimaryKeySelective(order)) throw new NewBeeMallException(ServiceResultEnum.DB_ERROR.getResult());
            //未支付状态转为已支付状态成功时，将付款记录表中的状态改为已付款
            if(alipayPayRecordService.selectByOrderNo(order.getOrderNo()) != null){
                if(alipayPayRecordService.updateStatus(Constants.ALIPAY_STATUS_PAYED, order.getOrderNo()) < 1){
                    NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
                }
            }
            logger.info("订单号为" + order.getOrderNo() + "的订单由未支付状态转为已支付状态");
        }catch (Exception e){
            e.printStackTrace();
            logger.error("订单号为" + order.getOrderNo() + "的订单转换支付状态失败，原因：数据库存在异常");
            StringBuffer key = new StringBuffer();
            key.append(Constants.PAY + ".");
            key.append(order.getUserId() + ".");
            key.append(order.getOrderNo() + ".");
            key.append(MD5Util.getSuffix());
            try{
                //未支付转已支付出现异常，直接去除redis的已支付信息
                if(redisCache.isExist(key.toString())) redisCache.deleteObject(key.toString());
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
