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
import ltd.newbee.mall.util.MD5Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RabbitListener(queues = RabbitmqConstant.ORDER_INSERT_QUEUE)
public class OrderInsertListener {

    private static Logger logger = LoggerFactory.getLogger(OrderInsertListener.class);

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private NewBeeMallOrderMapper newBeeMallOrderMapper;

    //TODO 出错可以考虑去除相关的订单项和购物车存储数据
    @RabbitHandler
    public void receiveInsertOrder(String orderInJson) throws IOException {
        NewBeeMallOrder order = JSON.parseObject(orderInJson, NewBeeMallOrder.class);
        try{
            int row = newBeeMallOrderMapper.insertSelective(order);
            if(row < 1) throw new NewBeeMallException(ServiceResultEnum.DB_ERROR.getResult());
            logger.info("订单号为" + order.getOrderNo() + "的订单插入成功");
        }catch (Exception e){
            e.printStackTrace();
            logger.error("订单号为" + order.getOrderNo() + "的订单插入失败，原因：数据库存在异常");
            StringBuffer key = new StringBuffer();
            key.append(Constants.PAY + ".");
            key.append(order.getUserId() + ".");
            key.append(order.getOrderNo() + ".");
            key.append(MD5Util.getSuffix());
            try{
                if(redisCache.isExist(key.toString())) redisCache.deleteObject(key.toString());
            }catch (Exception ex){
                NewBeeMallException.fail(ServiceResultEnum.REDIS_ERROR.getResult());
            }
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }
    }
}
