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
    public void receiveInsertOrder(String orderInJson, Message message, Channel channel) throws IOException {
        NewBeeMallOrder order = JSON.parseObject(orderInJson, NewBeeMallOrder.class);
        try{
            int row = newBeeMallOrderMapper.insertSelective(order);
            if(row < 1) throw new NewBeeMallException(ServiceResultEnum.DB_ERROR.getResult());
            logger.info("订单号为" + order.getOrderNo() + "的订单插入成功");
            //正常消费消息手动应答
            //第一个参数表示回应信息类型，第二个参数表示是否批量确认
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
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
            //消费消息异常
            //前两个参数同上，第三个参数表示是否重发消息，是则为true
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }
    }
}
