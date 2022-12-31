package ltd.newbee.mall.listener.rabbitmq;

import com.rabbitmq.client.Channel;
import ltd.newbee.mall.common.Constants;
import ltd.newbee.mall.common.NewBeeMallException;
import ltd.newbee.mall.common.NewBeeMallOrderStatusEnum;
import ltd.newbee.mall.common.ServiceResultEnum;
import ltd.newbee.mall.dao.NewBeeMallOrderMapper;
import ltd.newbee.mall.entity.NewBeeMallOrder;
import ltd.newbee.mall.rabbitmq.RabbitmqConstant;
import ltd.newbee.mall.service.AlipayPayRecordService;
import ltd.newbee.mall.service.NewBeeMallOrderService;
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
@RabbitListener(queues = RabbitmqConstant.ORDER_DELETE_QUEUE)
public class OrderTimeoutListener {

    private static Logger logger = LoggerFactory.getLogger(OrderTimeoutListener.class);

    //订单
    @Autowired
    private NewBeeMallOrderMapper newBeeMallOrderMapper;

    @Autowired
    private NewBeeMallOrderService newBeeMallOrderService;

    @Autowired
    private AlipayPayRecordService alipayPayRecordService;

    @RabbitHandler
    public void receiveTimeoutOrder(String orderNo, Message message, Channel channel) throws IOException {
        //超时关闭数据库更新
        NewBeeMallOrder order = newBeeMallOrderMapper.selectByOrderNo(orderNo);
        //除未支付和已支付订单会出现超时关闭的情况，别的都不会，故别的超时都不做处理
        if(!(order.getOrderStatus() == NewBeeMallOrderStatusEnum.ORDER_PRE_PAY.getOrderStatus()
                || order.getOrderStatus() == NewBeeMallOrderStatusEnum.ORDER_PAID.getOrderStatus())){
            return;
        }
        if(null == order){
            logger.error("订单号为" + orderNo + "的超时订单删除失败，原因：数据库中不存在待删除的订单信息");
            return;
        }
        try{
            if(order.getOrderStatus() == 1){
                //若为已支付的超时订单做退款标记
                newBeeMallOrderService.refund(orderNo, null);
            }
            //将订单状态改为-2（超时关闭）
            int row = newBeeMallOrderMapper.deleteByPrimaryKeyWhenTimeout(order.getOrderId());
            if(row < 1) throw new NewBeeMallException(ServiceResultEnum.DB_ERROR.getResult());
            logger.info("订单号为" + order.getOrderNo() + "的超时订单关闭成功");
            //正常消费消息手动应答
            //第一个参数表示回应信息类型，第二个参数表示是否批量确认
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        }catch (Exception e){
            e.printStackTrace();
            logger.error("订单号为" + orderNo + "的超时订单关闭失败，原因：数据库存在异常");
            //消费消息异常
            //前两个参数同上，第三个参数表示是否将消息重新入队，是则为true
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            throw e;
        }
    }
}
