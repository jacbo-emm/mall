package ltd.newbee.mall.service.impl;

import ltd.newbee.mall.common.NewBeeMallException;
import ltd.newbee.mall.controller.vo.NewBeeMallCouponVO;
import ltd.newbee.mall.controller.vo.NewBeeMallMyCouponVO;
import ltd.newbee.mall.controller.vo.NewBeeMallShoppingCartItemVO;
import ltd.newbee.mall.dao.NewBeeMallCouponMapper;
import ltd.newbee.mall.dao.NewBeeMallGoodsMapper;
import ltd.newbee.mall.dao.NewBeeMallUserCouponRecordMapper;
import ltd.newbee.mall.entity.NewBeeMallCoupon;
import ltd.newbee.mall.entity.NewBeeMallGoods;
import ltd.newbee.mall.entity.NewBeeMallUserCouponRecord;
import ltd.newbee.mall.service.NewBeeMallUserCouponService;
import ltd.newbee.mall.util.BeanUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
@Service
public class NewBeeMallUserCouponServiceImpl implements NewBeeMallUserCouponService {

    @Autowired
    private NewBeeMallUserCouponRecordMapper newBeeMallUserCouponRecordMapper;
    @Autowired
    private NewBeeMallCouponMapper newBeeMallCouponMapper;
    @Autowired
    private NewBeeMallGoodsMapper newBeeMallGoodsMapper;
    @Override
    /**
     * 查询展示优惠券
     */
    public List<NewBeeMallCouponVO> selectAvailableCoupon(Long userId) {
        //得到 未删除的（0） 可领用的（0） 未过期失效（0）
        //增加日期判断
        List<NewBeeMallCoupon> coupons = newBeeMallCouponMapper.selectAvailableCoupon();
        List<NewBeeMallCouponVO> couponVOS = BeanUtil.copyList(coupons, NewBeeMallCouponVO.class);
        for (NewBeeMallCouponVO couponVO : couponVOS) {
            if (userId != null) {
                int num = newBeeMallUserCouponRecordMapper.getUserCouponCount(userId, couponVO.getCouponId());
                if (num > 0) {
                    couponVO.setHasReceived(true);
                }
            }
            if (couponVO.getCouponTotal() != 0) {
                int count = newBeeMallUserCouponRecordMapper.getCouponCount(couponVO.getCouponId());
                if (count >= couponVO.getCouponTotal()) {
                    couponVO.setSaleOut(true);
                }
            }
        }
        return couponVOS;
    }

    @Override
    public boolean saveCouponUser(Long couponId, Long userId) {
        NewBeeMallCoupon newBeeMallCoupon = newBeeMallCouponMapper.selectByPrimaryKey(couponId);
        if (newBeeMallCoupon.getCouponLimit() != 0) {
            int num = newBeeMallUserCouponRecordMapper.getUserCouponCount(userId, couponId);
            if (num != 0) {
                throw new NewBeeMallException("优惠券已经领过了,无法再次领取！");
            }
        }
        if (newBeeMallCoupon.getCouponTotal() != 0) {
            int count = newBeeMallUserCouponRecordMapper.getCouponCount(couponId);
            if (count >= newBeeMallCoupon.getCouponTotal()) {
                throw new NewBeeMallException("优惠券已经领完了！");
            }
            if (newBeeMallCouponMapper.reduceCouponTotal(couponId) <= 0) {
                throw new NewBeeMallException("优惠券领取失败！");
            }
        }
        NewBeeMallUserCouponRecord couponUser = new NewBeeMallUserCouponRecord();
        couponUser.setUserId(userId);
        couponUser.setCouponId(couponId);
        return newBeeMallUserCouponRecordMapper.insertSelective(couponUser) > 0;
    }

    @Override
    public List<NewBeeMallCouponVO> selectMyCoupons(Long userId) {
        List<NewBeeMallUserCouponRecord> coupons = newBeeMallUserCouponRecordMapper.selectMyCoupons(userId);
        List<NewBeeMallCouponVO> couponVOS = new ArrayList<>();
        for (NewBeeMallUserCouponRecord couponUser : coupons) {
            NewBeeMallCoupon newBeeMallCoupon = newBeeMallCouponMapper.selectByPrimaryKey(couponUser.getCouponId());
            if (newBeeMallCoupon == null) {
                continue;
            }
            NewBeeMallCouponVO newBeeMallCouponVO = new NewBeeMallCouponVO();
            BeanUtil.copyProperties(newBeeMallCoupon, newBeeMallCouponVO);
            newBeeMallCouponVO.setCouponUserId(couponUser.getCouponUserId());
            newBeeMallCouponVO.setUsed(couponUser.getUsedTime() != null);
            couponVOS.add(newBeeMallCouponVO);
        }
        return couponVOS;
    }

    @Override
    public List<NewBeeMallMyCouponVO> selectOrderCanUseCoupons(List<NewBeeMallShoppingCartItemVO> myShoppingCartItems, int priceTotal, Long userId) {
        //用户-优惠券记录表
        List<NewBeeMallUserCouponRecord> couponUsers = newBeeMallUserCouponRecordMapper.selectMyAvailableCoupons(userId);
        List<NewBeeMallMyCouponVO> myCouponVOS = BeanUtil.copyList(couponUsers, NewBeeMallMyCouponVO.class);
        //优惠券ID
        List<Long> couponIds = couponUsers.stream().map(NewBeeMallUserCouponRecord::getCouponId).collect(Collectors.toList());


        if (!couponIds.isEmpty()) {
            ZoneId zone = ZoneId.systemDefault();
            List<NewBeeMallCoupon> coupons = newBeeMallCouponMapper.selectByIds(couponIds);
            for (NewBeeMallCoupon coupon : coupons) {
                for (NewBeeMallMyCouponVO myCouponVO : myCouponVOS) {
                    if (coupon.getCouponId().equals(myCouponVO.getCouponId())) {
                        myCouponVO.setName(coupon.getCouponName());
                        myCouponVO.setCouponDesc(coupon.getCouponDesc());
                        myCouponVO.setDiscount(coupon.getDiscount());
                        myCouponVO.setMin(coupon.getMin());
                        myCouponVO.setGoodsType(coupon.getGoodsType());
                        myCouponVO.setGoodsValue(coupon.getGoodsValue());
                        ZonedDateTime startZonedDateTime = coupon.getCouponStartTime().atStartOfDay(zone);
                        ZonedDateTime endZonedDateTime = coupon.getCouponEndTime().atStartOfDay(zone);
                        myCouponVO.setStartTime(Date.from(startZonedDateTime.toInstant()));
                        myCouponVO.setEndTime(Date.from(endZonedDateTime.toInstant()));
                    }
                }
            }
        }
        long nowTime = System.currentTimeMillis();
        return myCouponVOS.stream().filter(item -> {
            // 判断有效期
            Date startTime = item.getStartTime();
            Date endTime = item.getEndTime();
            if (startTime == null || endTime == null || nowTime < startTime.getTime() || nowTime > endTime.getTime()) {
                return false;
            }
            // 判断使用条件
            boolean b = false;
            if (item.getMin() <= priceTotal) {
                if (item.getGoodsType() == 1) { // 指定分类可用
                    String[] split = item.getGoodsValue().split(",");
                    List<Long> goodsValue = Arrays.stream(split).map(Long::valueOf).collect(Collectors.toList());
                    List<Long> goodsIds = myShoppingCartItems.stream().map(NewBeeMallShoppingCartItemVO::getGoodsId).collect(Collectors.toList());
                    List<NewBeeMallGoods> goods = newBeeMallGoodsMapper.selectByPrimaryKeys(goodsIds);
                    List<Long> categoryIds = goods.stream().map(NewBeeMallGoods::getGoodsCategoryId).collect(Collectors.toList());
                    for (Long categoryId : categoryIds) {
                        if (goodsValue.contains(categoryId)) {
                            b = true;
                            break;
                        }
                    }
                } else if (item.getGoodsType() == 2) { // 指定商品可用
                    String[] split = item.getGoodsValue().split(",");
                    List<Long> goodsValue = Arrays.stream(split).map(Long::valueOf).toList();
                    List<Long> goodsIds = myShoppingCartItems.stream().map(NewBeeMallShoppingCartItemVO::getGoodsId).toList();
                    /**
                     * 根据 goodId 查询得到 goodCategoryId 再判断 优惠券中 goodsValue是否包含 goodCategoryId
                     */
                    List<Long> goodCategoryIds = newBeeMallGoodsMapper.selectCategoryIdByGoodId(goodsIds);
                    for (Long goodCategryId:
                         goodCategoryIds) {
                        if(goodsValue.contains(goodCategryId)){
                            b = true;
                            break;
                        }
                    }
                    /*for (Long goodsId : goodsIds) {
                        if (goodsValue.contains(goodsId)) {
                            b = true;
                            break;
                        }
                    }*/
                } else { // 全场通用
                    b = true;
                }
            }
            return b;
        }).sorted(Comparator.comparingInt(NewBeeMallMyCouponVO::getDiscount)).collect(Collectors.toList());
    }

    @Override
    public boolean deleteCouponUser(Long couponUserId) {
        return newBeeMallUserCouponRecordMapper.deleteByPrimaryKey(couponUserId) > 0;
    }

    @Override
    public void releaseCoupon(Long orderId) {
        NewBeeMallUserCouponRecord newBeeMallUserCouponRecord = newBeeMallUserCouponRecordMapper.getUserCouponByOrderId(orderId);
        if (newBeeMallUserCouponRecord == null) {
            return;
        }
        newBeeMallUserCouponRecord.setUseStatus((byte) 0);
        newBeeMallUserCouponRecord.setUpdateTime(new Date());
        newBeeMallUserCouponRecordMapper.updateByPrimaryKey(newBeeMallUserCouponRecord);
    }

}
