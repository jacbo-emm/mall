package ltd.newbee.mall.service.impl;


import ltd.newbee.mall.dao.GoodsCategoryMapper;
import ltd.newbee.mall.dao.NewBeeMallCouponMapper;
import ltd.newbee.mall.entity.NewBeeMallCoupon;
import ltd.newbee.mall.service.NewBeeMallCouponService;
import ltd.newbee.mall.util.PageQueryUtil;
import ltd.newbee.mall.util.PageResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;


@Service
public class NewBeeMallCouponServiceImpl implements NewBeeMallCouponService {

    @Autowired
    private NewBeeMallCouponMapper newBeeMallCouponMapper;

    @Autowired
    private GoodsCategoryMapper goodsCategoryMapper;

    @Override
    public PageResult getCouponPage(PageQueryUtil pageUtil) {
        List<NewBeeMallCoupon> carousels = newBeeMallCouponMapper.findCouponlList(pageUtil);
        int total = newBeeMallCouponMapper.getTotalCoupons(pageUtil);
        return new PageResult(carousels, total, pageUtil.getLimit(), pageUtil.getPage());
    }

    /**
     * 新增优惠券
     * @param newBeeMallCoupon
     * @return
     */
    @Override
    public boolean saveCoupon(NewBeeMallCoupon newBeeMallCoupon) {
        //根据goodsType 决定 goodsValue的存储
        Byte goodsType = newBeeMallCoupon.getGoodsType();
        //goodsValue 可能为一个或多个，使用逗号分隔。(中英文都匹配)
        String[] goodsValues = newBeeMallCoupon.getGoodsValue().split("[，"+"\\"+",]");

        //判断是否为空
        if(goodsValues.length <= 0){
            return false;
        }
        // 全品类通用

        //指定类别或商品可用
        //指定某一类商品/某种商品可用可用
        StringBuffer gvalue = new StringBuffer();
        if(goodsValues.length == 1){
            gvalue.append(goodsValues[0]+",");
            if(goodsType == 1){
                Long[] categoryIds = goodsCategoryMapper.getCategoryIdByParentId(Long.parseLong(goodsValues[0]));
                if(categoryIds.length > 0){
                    for (int i = 0; i < categoryIds.length; i++) {
                        if (i < categoryIds.length-1){
                            gvalue.append(categoryIds[i]+",");
                        }else {
                            gvalue.append(categoryIds[i]);
                        }
                    }
                }
            }
            newBeeMallCoupon.setGoodsValue(gvalue.toString());
        }
        //指定多个种类商品 通用优惠券
        List<Long> cateGoryParentIds = new ArrayList();
        if(goodsValues.length > 1){
            for (int i = 0; i < goodsValues.length; i++) {
                gvalue.append(goodsValues[i]+",");
                cateGoryParentIds.add(Long.parseLong(goodsValues[i]));
            }
            if(goodsType == 1){
                Long[] categoryIds = goodsCategoryMapper.getCategoryIdByParentIds(cateGoryParentIds);
                for (int i = 0; i < categoryIds.length; i++) {
                    if(i < categoryIds.length-1){
                        gvalue.append(categoryIds[i]+",");
                    }else {
                        gvalue.append(categoryIds[i]);
                    }
                }
            }
            newBeeMallCoupon.setGoodsValue(gvalue.toString());
        }
        return newBeeMallCouponMapper.insertSelective(newBeeMallCoupon) > 0;
    }

    @Override
    public boolean updateCoupon(NewBeeMallCoupon newBeeMallCoupon) {

        return newBeeMallCouponMapper.updateByPrimaryKeySelective(newBeeMallCoupon) > 0;
    }

    @Override
    public NewBeeMallCoupon getCouponById(Long id) {
        return newBeeMallCouponMapper.selectByPrimaryKey(id);
    }

    @Override
    public boolean deleteCouponById(Long id) {
        return newBeeMallCouponMapper.deleteByPrimaryKey(id) > 0;
    }

}
