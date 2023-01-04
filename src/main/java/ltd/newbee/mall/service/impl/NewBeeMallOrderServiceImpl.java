/**
 * 严肃声明：
 * 开源版本请务必保留此注释头信息，若删除我方将保留所有法律责任追究！
 * 本系统已申请软件著作权，受国家版权局知识产权以及国家计算机软件著作权保护！
 * 可正常分享和学习源码，不得用于违法犯罪活动，违者必究！
 * Copyright (c) 2019-2020 十三 all rights reserved.
 * 版权所有，侵权必究！
 */
package ltd.newbee.mall.service.impl;

import com.alibaba.fastjson.JSON;
import ltd.newbee.mall.common.*;
import ltd.newbee.mall.controller.mall.OrderController;
import ltd.newbee.mall.controller.vo.*;
import ltd.newbee.mall.dao.*;
import ltd.newbee.mall.entity.*;
import ltd.newbee.mall.rabbitmq.RabbitmqConstant;
import ltd.newbee.mall.redis.RedisCache;
import ltd.newbee.mall.service.NewBeeMallOrderService;
import ltd.newbee.mall.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

@Service
public class NewBeeMallOrderServiceImpl implements NewBeeMallOrderService {

    private static Logger logger = LoggerFactory.getLogger(NewBeeMallOrderServiceImpl.class);

    //订单
    @Autowired
    private NewBeeMallOrderMapper newBeeMallOrderMapper;
    //订单-商品
    @Autowired
    private NewBeeMallOrderItemMapper newBeeMallOrderItemMapper;
    //用户-商品 购物车
    @Autowired
    private NewBeeMallShoppingCartItemMapper newBeeMallShoppingCartItemMapper;
    //商品
    @Autowired
    private NewBeeMallGoodsMapper newBeeMallGoodsMapper;
    //用户-优惠券
    @Autowired
    private NewBeeMallUserCouponRecordMapper newBeeMallUserCouponRecordMapper;
    //优惠券
    @Autowired
    private NewBeeMallCouponMapper newBeeMallCouponMapper;
    //秒杀
    @Autowired
    private NewBeeMallSeckillMapper newBeeMallSeckillMapper;
    //秒杀成功后的记录表
    @Autowired
    private NewBeeMallSeckillSuccessMapper newBeeMallSeckillSuccessMapper;
    //阿里付款记录表
    @Autowired
    private AlipayPayRecordMapper alipayPayRecordMapper;
    //阿里退款记录表
    @Autowired
    private AlipayRefundRecordMapper alipayRefundRecordMapper;

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    //超时服务（改为使用redis监听过期key）
//    @Autowired
//    private TaskService taskService;

    @Override
    public int timeOutClose(List<Long> orderIds, Date date) {
        return newBeeMallOrderMapper.timeOutClose(orderIds, date);
    }

    @Override
    public Long selectMaxOrderId() {
        return newBeeMallOrderMapper.selectMaxOrderId();
    }

    //TODO 这种整体的查询后续可以改为es实现
    @Override
    public PageResult getNewBeeMallOrdersPage(PageQueryUtil pageUtil) {
        List<NewBeeMallOrder> newBeeMallOrders = newBeeMallOrderMapper.findNewBeeMallOrderList(pageUtil);
        int total = newBeeMallOrderMapper.getTotalNewBeeMallOrders(pageUtil);
        List<NewBeeMallOrderRefundVo> norvs = new ArrayList<>();
        //将订单对象和一个退款状态标志封装到一个新的封装对象中
        //根据退款表中的状态来判别
        for(NewBeeMallOrder o : newBeeMallOrders){
            NewBeeMallOrderRefundVo norv = new NewBeeMallOrderRefundVo();
            BeanUtil.copyProperties(o, norv);
            List<AlipayRefundRecord> refundRecords = alipayRefundRecordMapper.selectByOrderNo(o.getOrderNo());
            //不存在记录则直接使用默认的-1
            if(refundRecords != null && refundRecords.size() > 0){
                boolean flag = true;
                for(AlipayRefundRecord r : refundRecords){
                    //只要出现一个未退款记录则为订单记录定为未退款记录
                    if(r.getStatus() == 0){
                        flag = false;
                        break;
                    }
                }
                //存在退款记录且不存在任何未退款记录时记为已退款（这里也是只有存在全额退款记录的时候才能成立）
                //否则则应该将已退款划分为部分退款和全额退款
                norv.setRefundStatus((byte) (flag ? 1 : 0));
            }
            norvs.add(norv);
        }

        return new PageResult(norvs, total, pageUtil.getLimit(), pageUtil.getPage());
    }

    //TODO 这种整体的查询后续可以改为es实现
    @Override
    public PageResult getMyOrders(PageQueryUtil pageUtil) {
        int total = newBeeMallOrderMapper.getTotalNewBeeMallOrders(pageUtil);
        List<NewBeeMallOrder> newBeeMallOrders = newBeeMallOrderMapper.findNewBeeMallOrderList(pageUtil);
        List<NewBeeMallOrderListVO> orderListVOS = new ArrayList<>();
        if (total > 0) {
            // 数据转换 将实体类转成vo
            orderListVOS = BeanUtil.copyList(newBeeMallOrders, NewBeeMallOrderListVO.class);
            // 设置订单状态中文显示值
            for (NewBeeMallOrderListVO newBeeMallOrderListVO : orderListVOS) {
                newBeeMallOrderListVO.setOrderStatusString(NewBeeMallOrderStatusEnum.getNewBeeMallOrderStatusEnumByStatus(newBeeMallOrderListVO.getOrderStatus()).getName());
            }
            List<Long> orderIds = newBeeMallOrders.stream().map(NewBeeMallOrder::getOrderId).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(orderIds)) {
                List<NewBeeMallOrderItem> orderItems = newBeeMallOrderItemMapper.selectByOrderIds(orderIds);
                Map<Long, List<NewBeeMallOrderItem>> itemByOrderIdMap = orderItems.stream().collect(groupingBy(NewBeeMallOrderItem::getOrderId));
                for (NewBeeMallOrderListVO newBeeMallOrderListVO : orderListVOS) {
                    // 封装每个订单列表对象的订单项数据
                    if (itemByOrderIdMap.containsKey(newBeeMallOrderListVO.getOrderId())) {
                        List<NewBeeMallOrderItem> orderItemListTemp = itemByOrderIdMap.get(newBeeMallOrderListVO.getOrderId());
                        // 将NewBeeMallOrderItem对象列表转换成NewBeeMallOrderItemVO对象列表
                        List<NewBeeMallOrderItemVO> newBeeMallOrderItemVOS = BeanUtil.copyList(orderItemListTemp, NewBeeMallOrderItemVO.class);
                        newBeeMallOrderListVO.setNewBeeMallOrderItemVOS(newBeeMallOrderItemVOS);
                    }
                }
            }
        }
        return new PageResult(orderListVOS, total, pageUtil.getLimit(), pageUtil.getPage());
    }

    @Override
    @Transactional
    public String updateOrderInfo(NewBeeMallOrder newBeeMallOrder) {

        NewBeeMallOrder temp = newBeeMallOrderMapper.selectByPrimaryKey(newBeeMallOrder.getOrderId());
        // 不为空且orderStatus>=0且状态为出库之前可以修改部分信息
        if (temp != null) {
            if(!(temp.getOrderStatus() >= 0 && temp.getOrderStatus() < 3)) return "订单已关闭，无法编辑";
            temp.setTotalPrice(newBeeMallOrder.getTotalPrice());
            temp.setUserAddress(newBeeMallOrder.getUserAddress());
            temp.setUpdateTime(new Date());
            //更新数据库信息成功后，更新redis中的key的value
            if (newBeeMallOrderMapper.updateByPrimaryKeySelective(temp) > 0) {
                String payKey = getOrderKeyByRedis(temp.getOrderNo());
                if(payKey == null){
                    if(temp.getOrderStatus() != 2) return "订单已关闭，无法编辑";
                    String key = getPayKey(temp);
                    redisCache.setCacheObject(key, JSON.toJSONString(temp), 1, TimeUnit.DAYS);
                }
                else{
                    Long expire = redisCache.getExpire(payKey, TimeUnit.SECONDS);
                    redisCache.setCacheObject(payKey, JSON.toJSONString(temp), expire, TimeUnit.SECONDS);
                }
                return ServiceResultEnum.SUCCESS.getResult();
            }
            return ServiceResultEnum.DB_ERROR.getResult();
        }
        return ServiceResultEnum.DATA_NOT_EXIST.getResult();
    }

    //订单配货（存redis的key，过期时间为15天，过期了也可以在数据库取，更新redis）
    @Override
    @Transactional
    public String checkDone(Long[] ids) {
        // 查询所有的订单 判断状态 修改状态和更新时间
        List<NewBeeMallOrder> orders = newBeeMallOrderMapper.selectByPrimaryKeys(Arrays.asList(ids));
        StringBuilder errorOrderNos = new StringBuilder();
        if (!CollectionUtils.isEmpty(orders)) {
            for (NewBeeMallOrder newBeeMallOrder : orders) {
                if (newBeeMallOrder.getIsDeleted() == 1) {
                    errorOrderNos.append(newBeeMallOrder.getOrderNo()).append(" ");
                    continue;
                }
                if (newBeeMallOrder.getOrderStatus() != 1) {
                    errorOrderNos.append(newBeeMallOrder.getOrderNo()).append(" ");
                }
            }
            if (StringUtils.isEmpty(errorOrderNos.toString())) {
                Date d = new Date();
                // 订单状态正常 可以执行配货完成操作 修改订单状态和更新时间
                if (newBeeMallOrderMapper.checkDone(Arrays.asList(ids), d) > 0) {
                    for(NewBeeMallOrder order : orders){
                        updateOrderStatusInRedis(d, order, (byte) NewBeeMallOrderStatusEnum.ORDER_PACKAGED.getOrderStatus(), 10l, TimeUnit.DAYS);
                    }
                    return ServiceResultEnum.SUCCESS.getResult();
                } else {
                    return ServiceResultEnum.DB_ERROR.getResult();
                }
            } else {
                // 订单此时不可执行出库操作
                if (errorOrderNos.length() > 0 && errorOrderNos.length() < 100) {
                    return errorOrderNos + "订单的状态不是支付成功无法执行出库操作";
                } else {
                    return "你选择了太多状态不是支付成功的订单，无法执行配货完成操作";
                }
            }
        }
        // 未查询到数据 返回错误提示
        return ServiceResultEnum.DATA_NOT_EXIST.getResult();
    }

    //订单出库（存redis的key，过期时间为5天，过期了也可以在数据库取，更新redis）
    @Override
    @Transactional
    public String checkOut(Long[] ids) {
        // 查询所有的订单 判断状态 修改状态和更新时间
        List<NewBeeMallOrder> orders = newBeeMallOrderMapper.selectByPrimaryKeys(Arrays.asList(ids));
        StringBuilder errorOrderNos = new StringBuilder();
        if (!CollectionUtils.isEmpty(orders)) {
            for (NewBeeMallOrder newBeeMallOrder : orders) {
                if (newBeeMallOrder.getIsDeleted() == 1) {
                    errorOrderNos.append(newBeeMallOrder.getOrderNo()).append(" ");
                    continue;
                }
                if (newBeeMallOrder.getOrderStatus() != 1 && newBeeMallOrder.getOrderStatus() != 2) {
                    errorOrderNos.append(newBeeMallOrder.getOrderNo()).append(" ");
                }
            }
            if (StringUtils.isEmpty(errorOrderNos.toString())) {
                // 订单状态正常 可以执行出库操作 修改订单状态和更新时间
                Date d = new Date();
                if (newBeeMallOrderMapper.checkOut(Arrays.asList(ids), d) > 0) {
                    for(NewBeeMallOrder order : orders){
                        updateOrderStatusInRedis(d, order, (byte) NewBeeMallOrderStatusEnum.ORDER_EXPRESS.getOrderStatus(), 30l, TimeUnit.DAYS);
                    }
                    return ServiceResultEnum.SUCCESS.getResult();
                } else {
                    return ServiceResultEnum.DB_ERROR.getResult();
                }
            } else {
                // 订单此时不可执行出库操作
                if (errorOrderNos.length() > 0 && errorOrderNos.length() < 100) {
                    return errorOrderNos + "订单的状态不是支付成功或配货完成无法执行出库操作";
                } else {
                    return "你选择了太多状态不是支付成功或配货完成的订单，无法执行出库操作";
                }
            }
        }
        // 未查询到数据 返回错误提示
        return ServiceResultEnum.DATA_NOT_EXIST.getResult();
    }

    //秒杀订单新增
    //TODO 订单项解耦
    @Override
    @Transactional
    public String seckillSaveOrder(Long seckillSuccessId, Long userId) {
        NewBeeMallSeckillSuccess newBeeMallSeckillSuccess = newBeeMallSeckillSuccessMapper.selectByPrimaryKey(seckillSuccessId);
        if (!newBeeMallSeckillSuccess.getUserId().equals(userId)) {
            throw new NewBeeMallException("当前登陆用户与抢购秒杀商品的用户不匹配");
        }
        Long seckillId = newBeeMallSeckillSuccess.getSeckillId();
        NewBeeMallSeckill newBeeMallSeckill = newBeeMallSeckillMapper.selectByPrimaryKey(seckillId);
        Long goodsId = newBeeMallSeckill.getGoodsId();
        NewBeeMallGoods newBeeMallGoods = newBeeMallGoodsMapper.selectByPrimaryKey(goodsId);
        // 生成订单号
        String orderNo = NumberUtil.genOrderNo();
        // 保存订单
        NewBeeMallOrder newBeeMallOrder = new NewBeeMallOrder();
        newBeeMallOrder.setOrderNo(orderNo);
        newBeeMallOrder.setTotalPrice(newBeeMallSeckill.getSeckillPrice());
        newBeeMallOrder.setUserId(userId);
        newBeeMallOrder.setUserAddress("秒杀测试地址");
        newBeeMallOrder.setOrderStatus((byte) NewBeeMallOrderStatusEnum.ORDER_PAID.getOrderStatus());
        newBeeMallOrder.setPayStatus((byte) PayStatusEnum.PAY_SUCCESS.getPayStatus());
        newBeeMallOrder.setPayType((byte) PayTypeEnum.WEIXIN_PAY.getPayType());
        newBeeMallOrder.setPayTime(new Date());
        String extraInfo = "";
        newBeeMallOrder.setExtraInfo(extraInfo);
        if (newBeeMallOrderMapper.insertSelective(newBeeMallOrder) <= 0) {
            throw new NewBeeMallException("生成订单内部异常");
        }
        // 保存订单商品项
        NewBeeMallOrderItem newBeeMallOrderItem = new NewBeeMallOrderItem();
        Long orderId = newBeeMallOrder.getOrderId();
        newBeeMallOrderItem.setOrderId(orderId);
        newBeeMallOrderItem.setSeckillId(seckillId);
        newBeeMallOrderItem.setGoodsId(newBeeMallGoods.getGoodsId());
        newBeeMallOrderItem.setGoodsCoverImg(newBeeMallGoods.getGoodsCoverImg());
        newBeeMallOrderItem.setGoodsName(newBeeMallGoods.getGoodsName());
        newBeeMallOrderItem.setGoodsCount(1);
        newBeeMallOrderItem.setSellingPrice(newBeeMallSeckill.getSeckillPrice());
        if (newBeeMallOrderItemMapper.insert(newBeeMallOrderItem) <= 0) {
            throw new NewBeeMallException("生成订单内部异常");
        }
//        // 订单支付超期任务
//        taskService.addTask(new OrderUnPaidTask(newBeeMallOrder.getOrderId(), 30 * 1000));
        return orderNo;
    }

    //新增订单
    @Override
    @Transactional(rollbackFor = Exception.class)
    public String saveOrder(NewBeeMallUserVO user, Long couponUserId, List<NewBeeMallShoppingCartItemVO> myShoppingCartItems) {
        // 生成订单号
        String orderNo = NumberUtil.genOrderNo();
        //防止插入重复订单
        if(getOrderKeyByRedis(orderNo) != null) NewBeeMallException.fail("订单已存在，切勿重复提交");
        //购物项id集合
        List<Long> itemIdList = myShoppingCartItems.stream().map(NewBeeMallShoppingCartItemVO::getCartItemId).collect(Collectors.toList());
        //商品id集合
        List<Long> goodsIds = myShoppingCartItems.stream().map(NewBeeMallShoppingCartItemVO::getGoodsId).collect(Collectors.toList());
        //查找该购物车含有的所有商品信息
        List<NewBeeMallGoods> newBeeMallGoods = newBeeMallGoodsMapper.selectByPrimaryKeys(goodsIds);

        //判断后续用来进行判断的购物项集合、商品id集合、商品集合是否为空
        if (CollectionUtils.isEmpty(itemIdList) || CollectionUtils.isEmpty(goodsIds) || CollectionUtils.isEmpty(newBeeMallGoods)) {
            NewBeeMallException.fail(ServiceResultEnum.ORDER_GENERATE_ERROR.getResult());
        }

        // 检查是否包含已下架商品
        List<NewBeeMallGoods> goodsListNotSelling = newBeeMallGoods.stream()
                .filter(newBeeMallGoodsTemp -> newBeeMallGoodsTemp.getGoodsSellStatus() != Constants.SELL_STATUS_UP)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(goodsListNotSelling)) {
            // goodsListNotSelling 对象非空则表示有下架商品
            NewBeeMallException.fail(goodsListNotSelling.get(0).getGoodsName() + "已下架，无法生成订单");
        }

        //将某个商品取出其id，然后将id和其对应该商品封装成Map的一条数据，为了重新建立id-实体关联,方便提高查询速度
        Map<Long, NewBeeMallGoods> newBeeMallGoodsMap = newBeeMallGoods.stream().collect(Collectors.toMap(NewBeeMallGoods::getGoodsId, Function.identity(), (entity1, entity2) -> entity1));
        // 判断商品库存
        for (NewBeeMallShoppingCartItemVO shoppingCartItemVO : myShoppingCartItems) {
            // 查出的商品中不存在购物车中的这条关联商品数据，直接返回错误提醒
            if (!newBeeMallGoodsMap.containsKey(shoppingCartItemVO.getGoodsId())) {
                NewBeeMallException.fail(ServiceResultEnum.SHOPPING_ITEM_ERROR.getResult());
            }
            // 存在数量大于库存的情况，直接返回错误提醒
            if (shoppingCartItemVO.getGoodsCount() > newBeeMallGoodsMap.get(shoppingCartItemVO.getGoodsId()).getStockNum()) {
                NewBeeMallException.fail(ServiceResultEnum.SHOPPING_ITEM_COUNT_ERROR.getResult());
            }
        }
        //提交订单时需要删除购物车中的购物项
        if (newBeeMallShoppingCartItemMapper.deleteBatch(itemIdList) <= 0) {
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }

        //StockNumDTO->goodsId、goodsCount
        //更新商品库存
        List<StockNumDTO> stockNumDTOS = BeanUtil.copyList(myShoppingCartItems, StockNumDTO.class);
        int updateStockNumResult = newBeeMallGoodsMapper.updateStockNum(stockNumDTOS);
        if (updateStockNumResult < 1) {
            NewBeeMallException.fail(ServiceResultEnum.SHOPPING_ITEM_COUNT_ERROR.getResult());
        }
        int priceTotal = 0;
        // 保存订单
        NewBeeMallOrder newBeeMallOrder = new NewBeeMallOrder();
        newBeeMallOrder.setOrderNo(orderNo);
        newBeeMallOrder.setUserId(user.getUserId());
        newBeeMallOrder.setUserAddress(user.getAddress());
        newBeeMallOrder.setCreateTime(new Date());
        // 总价
        for (NewBeeMallShoppingCartItemVO newBeeMallShoppingCartItemVO : myShoppingCartItems) {
            priceTotal += newBeeMallShoppingCartItemVO.getGoodsCount() * newBeeMallShoppingCartItemVO.getSellingPrice();
        }
        // 如果使用了优惠券
        if (couponUserId != null) {
            NewBeeMallUserCouponRecord newBeeMallUserCouponRecord = newBeeMallUserCouponRecordMapper.selectByPrimaryKey(couponUserId);
            NewBeeMallCoupon newBeeMallCoupon = newBeeMallCouponMapper.selectByPrimaryKey(newBeeMallUserCouponRecord.getCouponId());
            priceTotal -= newBeeMallCoupon.getDiscount();
        }
        //TODO 优惠券超额处理
        if (priceTotal < 1) {
            NewBeeMallException.fail(ServiceResultEnum.ORDER_PRICE_ERROR.getResult());
        }
        //订单总金额
        newBeeMallOrder.setTotalPrice(priceTotal);
        //订单描述信息
        String extraInfo = "newbeemall-plus支付宝沙箱支付";
        newBeeMallOrder.setExtraInfo(extraInfo);

        newBeeMallOrder.setUserName(user.getNickName());
        newBeeMallOrder.setUserPhone(user.getLoginName());

        // 生成订单
        try{
            insertOrder(newBeeMallOrder);
        }catch (Exception e){
            NewBeeMallException.fail(ServiceResultEnum.INSERT_ORDER_FAIL.getResult());
        }
//        if (newBeeMallOrderMapper.insertSelective(newBeeMallOrder) <= 0) {
//            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
//        }

        // 生成所有的订单项（订单-商品关联）快照，并保存至数据库
        //TODO 订单项可解耦
        List<NewBeeMallOrderItem> newBeeMallOrderItems = new ArrayList<>();
        for (NewBeeMallShoppingCartItemVO newBeeMallShoppingCartItemVO : myShoppingCartItems) {
            NewBeeMallOrderItem newBeeMallOrderItem = new NewBeeMallOrderItem();
            // 使用BeanUtil工具类将newBeeMallShoppingCartItemVO中的属性复制到newBeeMallOrderItem对象中
            BeanUtil.copyProperties(newBeeMallShoppingCartItemVO, newBeeMallOrderItem);
            // NewBeeMallOrderMapper文件insert()方法中使用了useGeneratedKeys因此orderId可以获取到
            newBeeMallOrderItem.setOrderId(newBeeMallOrder.getOrderId());
            newBeeMallOrderItems.add(newBeeMallOrderItem);
        }

        // 保存至数据库
        if (newBeeMallOrderItemMapper.insertBatch(newBeeMallOrderItems) <= 0) {
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }

        // 如果使用了优惠券，则更新优惠券状态
        if (couponUserId != null) {
            NewBeeMallUserCouponRecord couponUser = new NewBeeMallUserCouponRecord();
            couponUser.setCouponUserId(couponUserId);
            couponUser.setOrderId(newBeeMallOrder.getOrderId());
            couponUser.setUseStatus((byte) 1);
            couponUser.setUsedTime(new Date());
            couponUser.setUpdateTime(new Date());
            newBeeMallUserCouponRecordMapper.updateByPrimaryKeySelective(couponUser);
        }

//        // 订单支付超期任务，超过300秒自动取消订单
//        taskService.addTask(new OrderUnPaidTask(newBeeMallOrder.getOrderId(), ProjectConfig.getOrderUnpaidOverTime() * 1000));

        // 所有操作成功后，将订单号返回，以供Controller方法跳转到订单详情
        return orderNo;
    }


    //TODO 订单项解耦
    //通过订单no获取订单项
    @Override
    public NewBeeMallOrderDetailVO getOrderDetailByOrderNo(String orderNo, Long userId) {
        NewBeeMallOrder newBeeMallOrder = getOrderByRedis(orderNo);
        if (newBeeMallOrder == null) {
            NewBeeMallException.fail(ServiceResultEnum.ORDER_NOT_EXIST_ERROR.getResult());
        }
        //验证是否是当前userId下的订单，否则报错
        if (!userId.equals(newBeeMallOrder.getUserId())) {
            NewBeeMallException.fail(ServiceResultEnum.NO_PERMISSION_ERROR.getResult());
        }
        List<NewBeeMallOrderItem> orderItems = newBeeMallOrderItemMapper.selectByOrderId(newBeeMallOrder.getOrderId());
        //获取订单项数据
        if (CollectionUtils.isEmpty(orderItems)) {
            NewBeeMallException.fail(ServiceResultEnum.ORDER_ITEM_NOT_EXIST_ERROR.getResult());
        }
        List<NewBeeMallOrderItemVO> newBeeMallOrderItemVOS = BeanUtil.copyList(orderItems, NewBeeMallOrderItemVO.class);
        NewBeeMallOrderDetailVO newBeeMallOrderDetailVO = new NewBeeMallOrderDetailVO();
        BeanUtil.copyProperties(newBeeMallOrder, newBeeMallOrderDetailVO);
        newBeeMallOrderDetailVO.setOrderStatusString(NewBeeMallOrderStatusEnum.
                getNewBeeMallOrderStatusEnumByStatus(newBeeMallOrderDetailVO.getOrderStatus())
                .getName());
        newBeeMallOrderDetailVO.setPayTypeString(PayTypeEnum.getPayTypeEnumByType(newBeeMallOrderDetailVO.getPayType()).getName());
        newBeeMallOrderDetailVO.setNewBeeMallOrderItemVOS(newBeeMallOrderItemVOS);
        return newBeeMallOrderDetailVO;
    }

    //TODO redis查询
    @Override
    public List<NewBeeMallOrderItemVO> getOrderItems(Long id) {
        NewBeeMallOrder newBeeMallOrder = newBeeMallOrderMapper.selectByPrimaryKey(id);
        if (newBeeMallOrder != null) {
            List<NewBeeMallOrderItem> orderItems = newBeeMallOrderItemMapper.selectByOrderId(newBeeMallOrder.getOrderId());
            // 获取订单项数据
            if (!CollectionUtils.isEmpty(orderItems)) {
                return BeanUtil.copyList(orderItems, NewBeeMallOrderItemVO.class);
            }
        }
        return null;
    }

    //获取订单通过订单编号
    @Override
    public NewBeeMallOrder getNewBeeMallOrderByOrderNo(String orderNo) {
        return getOrderByRedis(orderNo);
    }

    //管理员/商户批量关闭订单（存redis的key，过期时间为2天+随机秒数（36000内），过期了也可以在数据库取，更新redis）
    @Override
    @Transactional
    public String closeOrder(Long[] ids) {
        // 查询所有的订单 判断状态 修改状态和更新时间
        List<NewBeeMallOrder> orders = newBeeMallOrderMapper.selectByPrimaryKeys(Arrays.asList(ids));

        StringBuilder errorOrderNos = new StringBuilder();
        if (!CollectionUtils.isEmpty(orders)) {
            for (NewBeeMallOrder newBeeMallOrder : orders) {
                // isDeleted=1 一定为已关闭订单
                if (newBeeMallOrder.getIsDeleted() == 1) {
                    errorOrderNos.append(newBeeMallOrder.getOrderNo()).append(" ");
                    continue;
                }
                // 关闭或者状态大过已支付状态皆不可关闭
                if (newBeeMallOrder.getOrderStatus() > 1 || newBeeMallOrder.getOrderStatus() < 0) {
                    errorOrderNos.append(newBeeMallOrder.getOrderNo()).append(" ");
                }
            }
            if (StringUtils.isEmpty(errorOrderNos.toString())) {
                // 订单状态正常 可以执行关闭操作 修改订单状态和更新时间
                //以后端项目时间为准
                Date d = new Date();
                if (newBeeMallOrderMapper.closeOrder(Arrays.asList(ids), NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_JUDGE.getOrderStatus(), d) > 0) {
                    //修改redis中的订单信息
                    for(NewBeeMallOrder order : orders){
                        //商户取消已支付订单则进行全额退款标记
                        if(order.getOrderStatus() == 1){
                            refund(order.getOrderNo(), null);
                        }
                        updateOrderStatusInRedis(d, order, (byte) NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_JUDGE.getOrderStatus(),86400l * 2 + ((long) Math.random() * 36001), TimeUnit.SECONDS);
                    }
                    return ServiceResultEnum.SUCCESS.getResult();
                } else {
                    return ServiceResultEnum.DB_ERROR.getResult();
                }
            } else {
                // 订单此时不可执行关闭操作
                if (errorOrderNos.length() > 0 && errorOrderNos.length() < 100) {
                    return errorOrderNos + "订单不能执行关闭操作";
                } else {
                    return "你选择的订单不能执行关闭操作";
                }
            }
        }
        // 未查询到数据 返回错误提示
        return ServiceResultEnum.DATA_NOT_EXIST.getResult();
    }

    //用户取消订单（存redis的key，过期时间为2天+随机秒数（36000内），过期了也可以在数据库取，更新redis）
    @Override
    @Transactional
    public String cancelOrder(String orderNo, Long userId) {
        NewBeeMallOrder newBeeMallOrder = getOrderByRedis(orderNo);
        if (newBeeMallOrder != null) {
            // 验证是否是当前userId下的订单，否则报错
            if (!userId.equals(newBeeMallOrder.getUserId())) {
                NewBeeMallException.fail(ServiceResultEnum.NO_PERMISSION_ERROR.getResult());
            }
            // 订单状态判断
            if (newBeeMallOrder.getOrderStatus().intValue() == NewBeeMallOrderStatusEnum.ORDER_SUCCESS.getOrderStatus()
                    || newBeeMallOrder.getOrderStatus().intValue() == NewBeeMallOrderStatusEnum.ORDER_PACKAGED.getOrderStatus()
                    || newBeeMallOrder.getOrderStatus().intValue() == NewBeeMallOrderStatusEnum.ORDER_EXPRESS.getOrderStatus()
                    || newBeeMallOrder.getOrderStatus().intValue() == NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_MALLUSER.getOrderStatus()
                    || newBeeMallOrder.getOrderStatus().intValue() == NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_EXPIRED.getOrderStatus()
                    || newBeeMallOrder.getOrderStatus().intValue() == NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_JUDGE.getOrderStatus()) {
                return ServiceResultEnum.ORDER_STATUS_ERROR.getResult();
            }
            Date d = new Date();
            if (newBeeMallOrderMapper.closeOrder(Collections.singletonList(newBeeMallOrder.getOrderId()), NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_MALLUSER.getOrderStatus(), d) > 0) {
                //用户取消已支付订单则进行全额退款标记
                if(newBeeMallOrder.getOrderStatus() == 1){
                    refund(orderNo, null);
                }
                updateOrderStatusInRedis(d, newBeeMallOrder, (byte) NewBeeMallOrderStatusEnum.ORDER_CLOSED_BY_MALLUSER.getOrderStatus(), 86400l * 2 + ((long) Math.random() * 36001), TimeUnit.SECONDS);
                return ServiceResultEnum.SUCCESS.getResult();
            } else {
                return ServiceResultEnum.DB_ERROR.getResult();
            }
        }
        return ServiceResultEnum.ORDER_NOT_EXIST_ERROR.getResult();
    }

    //用户订单完成（交易完成）（存redis的key，过期时间为之前状态的剩余过期时间，过期了也可以在数据库取，更新redis）
    @Override
    @Transactional
    public String finishOrder(String orderNo, Long userId) {
        NewBeeMallOrder newBeeMallOrder = getOrderByRedis(orderNo);
        if (newBeeMallOrder != null) {
            // 验证是否是当前userId下的订单，否则报错
            if (!userId.equals(newBeeMallOrder.getUserId())) {
                return ServiceResultEnum.NO_PERMISSION_ERROR.getResult();
            }
            // 订单状态判断 非出库状态下不进行修改操作
            if (newBeeMallOrder.getOrderStatus().intValue() != NewBeeMallOrderStatusEnum.ORDER_EXPRESS.getOrderStatus()) {
                return ServiceResultEnum.ORDER_STATUS_ERROR.getResult();
            }
            newBeeMallOrder.setOrderStatus((byte) NewBeeMallOrderStatusEnum.ORDER_SUCCESS.getOrderStatus());
            newBeeMallOrder.setUpdateTime(new Date());
            if (newBeeMallOrderMapper.updateByPrimaryKeySelective(newBeeMallOrder) > 0) {
                String payKey = getOrderKeyByRedis(newBeeMallOrder.getOrderNo());
                if(payKey == null){
                    payKey = getPayKey(newBeeMallOrder);
                    redisCache.setCacheObject(payKey, JSON.toJSONString(newBeeMallOrder), 1, TimeUnit.DAYS);
                }else{
                    Long expire = redisCache.getExpire(payKey, TimeUnit.SECONDS);
                    redisCache.setCacheObject(payKey, JSON.toJSONString(newBeeMallOrder), expire, TimeUnit.SECONDS);
                }
                return ServiceResultEnum.SUCCESS.getResult();
            } else {
                return ServiceResultEnum.DB_ERROR.getResult();
            }
        }
        return ServiceResultEnum.ORDER_NOT_EXIST_ERROR.getResult();
    }

    //更新订单状态
    private void updateOrderStatusInRedis(Date d, NewBeeMallOrder order, byte status, Long time, TimeUnit timeUnit) {
        order.setUpdateTime(d);
        order.setOrderStatus(status);
        String key = getPayKey(order);
        redisCache.setCacheObject(key, JSON.toJSONString(order), time, timeUnit);
    }

    //获得redis中的key（order对应key）
    private String getOrderKeyByRedis(String orderNo) {
        Set<String> orderKey = redisCache.keys("*" + orderNo + "*");
        String key = null;
        if(orderKey == null || orderKey.size() == 0){
            return null;
        }
        key = orderKey.iterator().next();
        return key;
    }

    //获取redis中的value（order对象）,查不到就在数据库查，然后过期时间置为1天加随机值（10小时中的任意秒数）
    private NewBeeMallOrder getOrderByRedis(String orderNo){
        String key = getOrderKeyByRedis(orderNo);
        if(key == null){
            NewBeeMallOrder o = newBeeMallOrderMapper.selectByOrderNo(orderNo);
            //未支付或者已支付的订单超时无法再放到redis中
            if(o.getOrderStatus() != NewBeeMallOrderStatusEnum.ORDER_PRE_PAY.getOrderStatus()
                && o.getOrderStatus() != NewBeeMallOrderStatusEnum.ORDER_PAID.getOrderStatus()
            ){
                redisCache.setCacheObject(getPayKey(o), JSON.toJSONString(o), 86400 + ((int) Math.random() * 36000), TimeUnit.SECONDS);
            }else{
                try{
                    //将订单状态改为-2（超时关闭）
                    int row = newBeeMallOrderMapper.deleteByPrimaryKeyWhenTimeout(o.getOrderId());
                    if(row < 1) throw new NewBeeMallException("");
                    logger.info("订单号为" + o.getOrderNo() + "的超时订单关闭成功");
                }catch (Exception e){
                    e.printStackTrace();
                    logger.error("订单号为" + orderNo + "的超时订单关闭失败，原因：数据库存在异常");
                    NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
                }
                return newBeeMallOrderMapper.selectByOrderNo(orderNo);
            }
            return o;
        }
        return JSON.parseObject(redisCache.getCacheObject(key), NewBeeMallOrder.class);
    }

    @Override
    public String paySuccess(String orderNo, int payType) {

        NewBeeMallOrder newBeeMallOrder = getOrderByRedis(orderNo);
//        NewBeeMallOrder newBeeMallOrder = newBeeMallOrderMapper.selectByOrderNo(orderNo);

        if (newBeeMallOrder == null) {
            return ServiceResultEnum.ORDER_NOT_EXIST_ERROR.getResult();
        }
        // 订单状态判断 非待支付状态下不进行修改操作
        if (newBeeMallOrder.getOrderStatus().intValue() != NewBeeMallOrderStatusEnum.ORDER_PRE_PAY.getOrderStatus()) {
            return ServiceResultEnum.ORDER_STATUS_ERROR.getResult();
        }
        newBeeMallOrder.setOrderStatus((byte) NewBeeMallOrderStatusEnum.ORDER_PAID.getOrderStatus());
        newBeeMallOrder.setPayType((byte) payType);
        newBeeMallOrder.setPayStatus((byte) PayStatusEnum.PAY_SUCCESS.getPayStatus());
        newBeeMallOrder.setPayTime(new Date());
        newBeeMallOrder.setUpdateTime(new Date());

        //将付款记录表中的状态改为已付款
        if(alipayPayRecordMapper.selectByOrderNo(newBeeMallOrder.getOrderNo()) != null){
            if(alipayPayRecordMapper.updateStatus(Constants.ALIPAY_STATUS_PAYED, newBeeMallOrder.getOrderNo()) < 1){
                return "付款记录状态更新失败";
            }
        }else{
            return "付款记录不存在";
        }
        //生成退款记录
        if(newBeeMallOrder.getOrderStatus() == 1){
            //若为已支付订单则生成退款标记
            try {
                refund(newBeeMallOrder.getOrderNo(), null);
            }catch (Exception e){
                return "生成退款记录失败";
            }
        }

        // 未支付订单改为已支付订单
        try{
            unpayToPayed(newBeeMallOrder);
        }catch (Exception e){
            return "更新未支付订单为已支付订单失败";
        }
//        if (newBeeMallOrderMapper.updateByPrimaryKeySelective(newBeeMallOrder) <= 0) {
//            return ServiceResultEnum.DB_ERROR.getResult();
//        }

//        //移除超时任务
//        taskService.removeTask(new OrderUnPaidTask(newBeeMallOrder.getOrderId()));
        return ServiceResultEnum.SUCCESS.getResult();
    }


    //TODO 用到才进行实现的更新方法（此处没有用到）
    @Override
    @Transactional
    public boolean updateByPrimaryKeySelective(NewBeeMallOrder newBeeMallOrder) {
        return newBeeMallOrderMapper.updateByPrimaryKeySelective(newBeeMallOrder) > 0;
    }

    @Override
    //第二个参数为需要退款的值，为空则退全款
    //进行退款标识(成功不报错，失败报错)
    //数据库退款记录表中退款状态为已退款或为未退款的记录中的退款值都叫做退款标记额
    public void refund(String orderNo, String needRefundAmount){
        //查找先前的付款记录
        AlipayPayRecord record = alipayPayRecordMapper.selectByOrderNo(orderNo);
        if(record == null) NewBeeMallException.fail(ServiceResultEnum.REFUND_ERROR.getResult());
        //实际付款额
        BigDecimal recordAmount = BigDecimal.valueOf(Double.valueOf(record.getTotalAmount()));

        //获取已存在的退款条目，观察是否还能继续生成退款条目
        List<AlipayRefundRecord> refundRecords = alipayRefundRecordMapper.selectByOrderNo(orderNo);
        //条目存在
        if(refundRecords != null && refundRecords.size() > 0){
            //统计已存在的退款标记额，得到退款标记总额
            BigDecimal res = BigDecimal.ZERO;
            for(AlipayRefundRecord refundRecord : refundRecords){
                //BigDecimal特性，只能这样加
                res = res.add(BigDecimal.valueOf(Double.valueOf(refundRecord.getRefundAmount())));
            }
            if(res.compareTo(recordAmount) == 0) NewBeeMallException.fail("退款标记总额已满足付款额，可以发起退款");
            else{
                //如果传入值为null，表示退剩下的没退的部分的值
                if(needRefundAmount == null){
                    needRefundAmount = recordAmount.subtract(res).abs().toString();
                }else{
                //如果传入值不为null，则看看加上传入值，退款总额会不会大过付款额
                    res = res.add(BigDecimal.valueOf(Double.valueOf(needRefundAmount)));
                    //退款总额大过付款额则抛出错误
                    if(res.compareTo(recordAmount) > 0){
                        NewBeeMallException.fail("退款标记总额不能大过付款额");
                    }
                }
            }
        }else{
            //条目不存在
            if(needRefundAmount == null){
                //退款额即为付款额
                needRefundAmount = recordAmount.toString();
            }else{
                if((BigDecimal.valueOf(Double.valueOf(needRefundAmount))).compareTo(recordAmount) > 0){
                    NewBeeMallException.fail("退款标记总额不能大过付款额");
                }
            }
        }

        //通过付款记录生成退款记录
        AlipayRefundRecord refundRecord = new AlipayRefundRecord();
        BeanUtil.copyProperties(record, refundRecord);
        refundRecord.setStatus(Constants.ALIPAY_STATUS_UNREFUND);
        refundRecord.setRefundAmount(needRefundAmount);
        refundRecord.setOutRequestNo(UUID.randomUUID().toString());
        //将需要退款的退款记录记录下来
        if(alipayRefundRecordMapper.insertSelective(refundRecord) < 1){
            NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
        }
    }

    @Override
    public List<NewBeeMallOrder> selectAllOrderIsPayedAndUnpay() {
        return newBeeMallOrderMapper.selectAllOrderIsPayedAndUnpay();
    }

    //从数据库里取，来保证退款的准确性
    @Override
    public NewBeeMallOrder getOrderByOrderNo(String orderNo) {
        return newBeeMallOrderMapper.selectByOrderNo(orderNo);
    }

    //退款额为全额的更新
    @Override
    public String refundSuccess(String orderNo) {
        if(alipayRefundRecordMapper.updateStatusByOrderNo(new Date(), Constants.ALIPAY_STATUS_REFUNDED, orderNo) < 1){
            return "更新数据库异常";
        }
        return ServiceResultEnum.SUCCESS.getResult();
    }

    //从这以下是防止单个方法过多代码的代码抽取，增加可读性
    //新建订单信息存到redis，并使用mq进行延迟新增订单信息到数据库
    private void insertOrder(NewBeeMallOrder order) {
        Long orderId = null;
        if(!redisCache.isExist(Constants.ORDER_ID_COUNT)){
            Long newOrderCount = newBeeMallOrderMapper.selectMaxOrderId();
            redisCache.setCacheObject(Constants.ORDER_ID_COUNT, newOrderCount + 1);
            orderId = newOrderCount + 1;
        }else{
            Long newOrderCount = (Long) redisCache.getCacheObject(Constants.ORDER_ID_COUNT);
            orderId = newOrderCount + 1;
            redisCache.setCacheObject(Constants.ORDER_ID_COUNT, orderId);
        }
        //设置订单id
        order.setOrderId(orderId);
        //设置新增订单默认值
        order.setPayStatus((byte) 0);
        order.setPayType((byte) 0);
        order.setOrderStatus((byte) 0);
        order.setIsDeleted((byte) 0);
        order.setUpdateTime(order.getCreateTime());
        //将未支付的订单信息存到redis中,设置信息的过期时间,且将信息传递到mq中
        unpaySave(order);
    }
    //新建订单的mq解耦逻辑
    private void unpaySave(NewBeeMallOrder order){
        String orderKey = getPayKey(order);
        String orderInJson = JSON.toJSONString(order);
        //过期时间为30分钟+20秒
        redisCache.setCacheObject(orderKey, orderInJson, 1820, TimeUnit.SECONDS);
        rabbitTemplate.convertAndSend(RabbitmqConstant.ORDER_EXCHANGE,
                RabbitmqConstant.ORDER_INSERT_ROUTE_KEY,
                orderInJson
                );
    }

    //将订单由未支付转为已支付，用mq解耦更新过程
    private void unpayToPayed(NewBeeMallOrder order){
        payedSave(order);
    }

    private void payedSave(NewBeeMallOrder order){
        String orderKey = getOrderKeyByRedis(order.getOrderNo());
        if(orderKey == null) orderKey = getPayKey(order);
        String orderInJson = JSON.toJSONString(order);
        redisCache.setCacheObject(orderKey, orderInJson, 15, TimeUnit.DAYS);
        rabbitTemplate.convertAndSend(RabbitmqConstant.ORDER_EXCHANGE,
                RabbitmqConstant.ORDER_UPDATE_ROUTE_KEY,
                orderInJson
        );
    }


    //通过实体类获取拼好的支付key
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
