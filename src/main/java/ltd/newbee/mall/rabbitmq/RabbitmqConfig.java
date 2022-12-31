package ltd.newbee.mall.rabbitmq;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitmqConfig {
    //订单增删改队列
    @Bean
    public Queue orderInsertQueue(){
        return new Queue(RabbitmqConstant.ORDER_INSERT_QUEUE,true);
    }
    @Bean
    public Queue orderDeleteQueue(){
        return new Queue(RabbitmqConstant.ORDER_DELETE_QUEUE,true);
    }
    @Bean
    public Queue orderUpdateQueue(){
        return new Queue(RabbitmqConstant.ORDER_UPDATE_QUEUE,true);
    }

    //订单交换机
    @Bean
    public TopicExchange orderExchange(){
        return new TopicExchange(RabbitmqConstant.ORDER_EXCHANGE);
    }

    //绑定订单队列和交换机
    @Bean
    public Binding bindingInsertOrder(){
        return BindingBuilder
                .bind(orderInsertQueue())
                .to(orderExchange())
                .with(RabbitmqConstant.ORDER_INSERT_ROUTE_KEY);
    }
    @Bean
    public Binding bindingDeleteOrder(){
        return BindingBuilder
                .bind(orderDeleteQueue())
                .to(orderExchange())
                .with(RabbitmqConstant.ORDER_DELETE_ROUTE_KEY);
    }
    @Bean
    public Binding bindingUpdateOrder(){
        return BindingBuilder
                .bind(orderUpdateQueue())
                .to(orderExchange())
                .with(RabbitmqConstant.ORDER_UPDATE_ROUTE_KEY);
    }

}
