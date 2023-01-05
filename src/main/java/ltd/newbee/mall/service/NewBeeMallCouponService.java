package ltd.newbee.mall.service;

import ltd.newbee.mall.controller.vo.NewBeeMallCouponVO;
import ltd.newbee.mall.controller.vo.NewBeeMallMyCouponVO;
import ltd.newbee.mall.controller.vo.NewBeeMallShoppingCartItemVO;
import ltd.newbee.mall.entity.NewBeeMallCoupon;
import ltd.newbee.mall.util.PageQueryUtil;
import ltd.newbee.mall.util.PageResult;

import java.util.List;

public interface NewBeeMallCouponService {

    PageResult getCouponPage(PageQueryUtil pageUtil);

    boolean saveCoupon(NewBeeMallCoupon newBeeMallCoupon);

    boolean updateCoupon(NewBeeMallCoupon newBeeMallCoupon);

    NewBeeMallCoupon getCouponById(Long id);

    boolean deleteCouponById(Long id);


}
