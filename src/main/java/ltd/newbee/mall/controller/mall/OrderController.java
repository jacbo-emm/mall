package ltd.newbee.mall.controller.mall;

import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayClient;
import com.alipay.api.DefaultAlipayClient;
import com.alipay.api.request.AlipayTradePagePayRequest;
import com.alipay.api.request.AlipayTradeRefundRequest;
import com.alipay.api.response.AlipayTradeRefundResponse;
import ltd.newbee.mall.annotion.RepeatSubmit;
import ltd.newbee.mall.common.*;
import ltd.newbee.mall.config.AlipayConfig;
import ltd.newbee.mall.config.ProjectConfig;
import ltd.newbee.mall.controller.vo.NewBeeMallOrderDetailVO;
import ltd.newbee.mall.controller.vo.NewBeeMallShoppingCartItemVO;
import ltd.newbee.mall.controller.vo.NewBeeMallUserVO;
import ltd.newbee.mall.dao.MallUserMapper;
import ltd.newbee.mall.entity.AlipayPayRecord;
import ltd.newbee.mall.entity.AlipayRefundRecord;
import ltd.newbee.mall.entity.NewBeeMallOrder;
import ltd.newbee.mall.service.AlipayPayRecordService;
import ltd.newbee.mall.service.AlipayRefundRecordService;
import ltd.newbee.mall.service.NewBeeMallOrderService;
import ltd.newbee.mall.service.NewBeeMallShoppingCartService;
import ltd.newbee.mall.util.MD5Util;
import ltd.newbee.mall.util.PageQueryUtil;
import ltd.newbee.mall.util.Result;
import ltd.newbee.mall.util.ResultGenerator;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Controller
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    @Autowired
    private NewBeeMallShoppingCartService newBeeMallShoppingCartService;
    @Autowired
    private NewBeeMallOrderService newBeeMallOrderService;

    @Autowired
    private AlipayPayRecordService alipayPayRecordService;

    @Autowired
    private AlipayRefundRecordService alipayRefundRecordService;

    @Autowired
    private MallUserMapper mallUserMapper;
    @Autowired
    private AlipayConfig alipayConfig;

    @Value("${payServerUrl}")
    private String payServerUrl;


    @GetMapping("/orders/{orderNo}")
    public String orderDetailPage(HttpServletRequest request, @PathVariable("orderNo") String orderNo, HttpSession httpSession) {
        NewBeeMallUserVO user = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        NewBeeMallOrderDetailVO orderDetailVO = newBeeMallOrderService.getOrderDetailByOrderNo(orderNo, user.getUserId());
        request.setAttribute("orderDetailVO", orderDetailVO);
        //当前订单记录如果存在退款记录且订单状态为关闭（用户、超时、商户）则加入一个标志位，用来展示退款按钮
        if(orderDetailVO.getOrderStatus() <= -1 && orderDetailVO.getOrderStatus() >= -3){
            List<AlipayRefundRecord> rr = alipayRefundRecordService.selectByOrderNo(orderNo);
            if(rr != null && rr.size() > 0){
                boolean flag = true;
                if(rr.get(0).getStatus() == 1) flag = false;
                request.setAttribute("canRefund", flag);
            }
        }
        return "mall/order-detail";
    }

    @GetMapping("/orders")
    public String orderListPage(@RequestParam Map<String, Object> params, HttpServletRequest request, HttpSession httpSession) {
        NewBeeMallUserVO user = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        params.put("userId", user.getUserId());
        if (StringUtils.isEmpty((CharSequence) params.get("page"))) {
            params.put("page", 1);
        }
        params.put("limit", Constants.ORDER_SEARCH_PAGE_LIMIT);
        //封装我的订单数据
        PageQueryUtil pageUtil = new PageQueryUtil(params);
        request.setAttribute("orderPageResult", newBeeMallOrderService.getMyOrders(pageUtil));
        request.setAttribute("path", "orders");
        return "mall/my-orders";
    }

    @RepeatSubmit
    @GetMapping("/saveOrder")
    public String saveOrder(Long couponUserId, HttpSession httpSession) {
        NewBeeMallUserVO user = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        List<NewBeeMallShoppingCartItemVO> myShoppingCartItems = newBeeMallShoppingCartService.getMyShoppingCartItems(user.getUserId());
        if (StringUtils.isEmpty(user.getAddress().trim())) {
            //无收货地址
            NewBeeMallException.fail(ServiceResultEnum.NULL_ADDRESS_ERROR.getResult());
        }
        if (CollectionUtils.isEmpty(myShoppingCartItems)) {
            //购物车中无数据则跳转至错误页
            NewBeeMallException.fail(ServiceResultEnum.SHOPPING_ITEM_ERROR.getResult());
        }
        //保存订单并返回订单号
        String saveOrderResult = newBeeMallOrderService.saveOrder(user, couponUserId, myShoppingCartItems);
        //跳转到订单详情页
        return "redirect:/orders/" + saveOrderResult;
    }

    @RepeatSubmit
    @GetMapping("/saveSeckillOrder/{seckillSuccessId}/{userId}/{seckillSecretKey}")
    public String saveOrder(@PathVariable Long seckillSuccessId,
                            @PathVariable Long userId,
                            @PathVariable String seckillSecretKey) {
        if (seckillSecretKey == null || !seckillSecretKey.equals(MD5Util.MD5Encode(seckillSuccessId + Constants.SECKILL_ORDER_SALT, Constants.UTF_ENCODING))) {
            throw new NewBeeMallException("秒杀商品下单不合法");
        }
        // 保存订单并返回订单号
        String saveOrderResult = newBeeMallOrderService.seckillSaveOrder(seckillSuccessId, userId);
        // 跳转到订单详情页
        return "redirect:/orders/" + saveOrderResult;
    }

    @RepeatSubmit
    @GetMapping("/selectPayType")
    public String selectPayType(HttpServletRequest request, @RequestParam("orderNo") String orderNo, HttpSession httpSession) {
        NewBeeMallUserVO user = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        NewBeeMallOrder newBeeMallOrder = judgeOrderUserId(orderNo, user.getUserId());
        //判断订单状态
        if (newBeeMallOrder.getOrderStatus().intValue() != NewBeeMallOrderStatusEnum.ORDER_PRE_PAY.getOrderStatus()) {
            NewBeeMallException.fail(ServiceResultEnum.ORDER_STATUS_ERROR.getResult());
        }
        request.setAttribute("orderNo", orderNo);
        request.setAttribute("totalPrice", newBeeMallOrder.getTotalPrice());
        return "mall/pay-select";
    }

    //支付宝的使用
    @RepeatSubmit
    @GetMapping("/payPage")
    public String payOrder(HttpServletRequest request, @RequestParam("orderNo") String orderNo, HttpSession httpSession, @RequestParam("payType") int payType) throws UnsupportedEncodingException {
        NewBeeMallUserVO mallUserVO = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        Long userId = mallUserVO.getUserId();
        NewBeeMallOrder newBeeMallOrder = judgeOrderUserId(orderNo, userId);

        AlipayPayRecord alipayPayRecord = alipayPayRecordService.selectByOrderNo(newBeeMallOrder.getOrderNo());
        //防止重复付款
        if(alipayPayRecord != null && alipayPayRecord.getStatus() == 1) throw new NewBeeMallException("订单已付款");

        // 判断订单userId(好像多余了)
        if (!userId.equals(newBeeMallOrder.getUserId())) {
            NewBeeMallException.fail(ServiceResultEnum.NO_PERMISSION_ERROR.getResult());
        }
        // 判断订单状态
        if (newBeeMallOrder.getOrderStatus() != NewBeeMallOrderStatusEnum.ORDER_PRE_PAY.getOrderStatus()
                || newBeeMallOrder.getPayStatus() != PayStatusEnum.PAY_ING.getPayStatus()) {
            throw new NewBeeMallException("订单结算异常");
        }

        request.setAttribute("orderNo", orderNo);
        request.setAttribute("totalPrice", newBeeMallOrder.getTotalPrice());
        if (payType == 1) {
            request.setCharacterEncoding(Constants.UTF_ENCODING);

            // 初始化
            AlipayClient alipayClient = new DefaultAlipayClient(alipayConfig.getGateway(), alipayConfig.getAppId(),
                    alipayConfig.getRsaPrivateKey(), alipayConfig.getFormat(), alipayConfig.getCharset(), alipayConfig.getAlipayPublicKey(),
                    alipayConfig.getSigntype());
            // 创建API对应的request
            AlipayTradePagePayRequest alipayRequest = new AlipayTradePagePayRequest();
            // 在公共参数中设置回跳和通知地址,通知地址需要公网可访问
//            String url = ProjectConfig.getServerUrl() + request.getContextPath();
            //用了内网穿透的url
            String url = payServerUrl + request.getContextPath();
            alipayRequest.setReturnUrl(url + "/returnOrders/" + newBeeMallOrder.getOrderNo() + "/" + userId);
            alipayRequest.setNotifyUrl(url + "/paySuccess?payType=1&orderNo=" + newBeeMallOrder.getOrderNo());

            // 填充业务参数
            // 必填
            // 商户订单号，需保证在商户端不重复
            String out_trade_no = "";

            // 销售产品码，与支付宝签约的产品码名称。目前仅支持FAST_INSTANT_TRADE_PAY
            String product_code = "";
            // 订单总金额，单位为元，精确到小数点后两位，取值范围[0.01,100000000]。
            String total_amount = "";
            // 订单标题
            String subject = "";

            // 选填
            // 商品描述，可空
            String body = "";

            //将当前付款信息放到记录表中
            //如果已经存在付款记录，则不添加
            if(alipayPayRecord == null){
                // 必填
                // 商户订单号，需保证在商户端不重复
                out_trade_no = newBeeMallOrder.getOrderNo() + new Random().nextInt(9999);

                // 销售产品码，与支付宝签约的产品码名称。目前仅支持FAST_INSTANT_TRADE_PAY
                product_code = "FAST_INSTANT_TRADE_PAY";
                // 订单总金额，单位为元，精确到小数点后两位，取值范围[0.01,100000000]。
                total_amount = newBeeMallOrder.getTotalPrice() + "";
                // 订单标题
                subject = "支付宝测试";

                // 选填
                // 商品描述，可空
                body = "商品描述";
                AlipayPayRecord apr = new AlipayPayRecord();
                apr.setOrderId(newBeeMallOrder.getOrderId());
                apr.setOrderNo(newBeeMallOrder.getOrderNo());
                apr.setOutTradeNo(out_trade_no);
                apr.setProductCode(product_code);
                apr.setTotalAmount(total_amount);
                apr.setSubject(subject);
                apr.setBody(body);
                if(alipayPayRecordService.insertSelective(apr) < 1) NewBeeMallException.fail(ServiceResultEnum.DB_ERROR.getResult());
            }else{
                // 必填
                // 商户订单号，需保证在商户端不重复
                out_trade_no = alipayPayRecord.getOutTradeNo();

                // 销售产品码，与支付宝签约的产品码名称。目前仅支持FAST_INSTANT_TRADE_PAY
                product_code = alipayPayRecord.getProductCode();
                // 订单总金额，单位为元，精确到小数点后两位，取值范围[0.01,100000000]。
                total_amount = alipayPayRecord.getTotalAmount();
                // 订单标题
                subject = alipayPayRecord.getSubject();

                // 选填
                // 商品描述，可空
                body = alipayPayRecord.getBody();
            }

            alipayRequest.setBizContent("{" + "\"out_trade_no\":\"" + out_trade_no + "\"," + "\"product_code\":\""
                    + product_code + "\"," + "\"total_amount\":\"" + total_amount + "\"," + "\"subject\":\"" + subject
                    + "\"," + "\"body\":\"" + body + "\"}");

            // 请求
            String form;
            try {
                // 需要自行申请支付宝的沙箱账号、申请appID，并在配置文件中依次配置AppID、密钥、公钥，否则这里会报错。
                form = alipayClient.pageExecute(alipayRequest).getBody();//调用SDK生成表单
                //表单封装到alipay.html模板中，产生一个实际页面进行支付宝付款请求
                request.setAttribute("form", form);
            } catch (AlipayApiException e) {
                e.printStackTrace();
            }
            return "mall/alipay";
        } else {
            return "mall/wxpay";
        }
    }

    @GetMapping("/returnOrders/{orderNo}/{userId}")
    public String returnOrderDetailPage(HttpServletRequest request, @PathVariable String orderNo, @PathVariable Long userId) {
        log.info("支付宝return通知数据记录：orderNo: {}, 当前登陆用户：{}", orderNo, userId);
        // NewBeeMallOrder newBeeMallOrder = judgeOrderUserId(orderNo, userId);
        // 将notifyUrl中逻辑放到此处：未支付订单更新订单状态
        NewBeeMallOrderDetailVO orderDetailVO = newBeeMallOrderService.getOrderDetailByOrderNo(orderNo, userId);
        if (orderDetailVO == null) {
            return "error/error_5xx";
        }
        request.setAttribute("orderDetailVO", orderDetailVO);
        //当前订单记录如果存在退款记录且订单状态为关闭（用户、超时、商户）则加入一个标志位，用来展示退款按钮
        if(orderDetailVO.getOrderStatus() <= -1 && orderDetailVO.getOrderStatus() >= -3){
            List<AlipayRefundRecord> rr = alipayRefundRecordService.selectByOrderNo(orderNo);
            if(rr != null && rr.size() > 0){
                boolean flag = true;
                if(rr.get(0).getStatus() == 1) flag = false;
                request.setAttribute("canRefund", flag);
            }
        }
        return "mall/order-detail";
    }

    @PostMapping("/paySuccess")
    @ResponseBody
    public Result paySuccess(Integer payType, String orderNo) {
        log.info("支付宝paySuccess通知数据记录：orderNo: {}, payType：{}", orderNo, payType);
        String payResult = newBeeMallOrderService.paySuccess(orderNo, payType);
        if (ServiceResultEnum.SUCCESS.getResult().equals(payResult)) {
            return ResultGenerator.genSuccessResult();
        } else {
            return ResultGenerator.genFailResult(payResult);
        }
    }

    @RepeatSubmit
    @PutMapping("/orders/{orderNo}/cancel")
    @ResponseBody
    public Result cancelOrder(@PathVariable("orderNo") String orderNo, HttpSession httpSession, HttpServletResponse response) {
        NewBeeMallUserVO user = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        String cancelOrderResult = newBeeMallOrderService.cancelOrder(orderNo, user.getUserId());
        if (ServiceResultEnum.SUCCESS.getResult().equals(cancelOrderResult)) {
            return ResultGenerator.genSuccessResult();
        } else {
            return ResultGenerator.genFailResult(cancelOrderResult);
        }
    }

    //支持用户订单被关闭后的对订单的退款，只支持全额退款
    @RepeatSubmit
    @PutMapping("/orders/{orderNo}/refund")
    @ResponseBody
    public Result refundOrder(@PathVariable("orderNo") String orderNo){
        //判断退款记录是否存在
        List<AlipayRefundRecord> refundRecords = alipayRefundRecordService.selectByOrderNo(orderNo);
        if(!(refundRecords != null && refundRecords.size() > 0)){
            return ResultGenerator.genFailResult("退款记录不存在，无法退款");
        }
        AlipayRefundRecord rr = refundRecords.get(0);
        //判断是否已进行过退款
        if(rr.getStatus() == 1){
            return ResultGenerator.genFailResult("已进行退款，无法重复进行退款");
        }
        //获得初始化的AlipayClient
        AlipayClient alipayClient = new DefaultAlipayClient(alipayConfig.getGateway(), alipayConfig.getAppId(),
                alipayConfig.getRsaPrivateKey(), alipayConfig.getFormat(), alipayConfig.getCharset(), alipayConfig.getAlipayPublicKey()
                ,alipayConfig.getSigntype());
        //设置请求参数
        AlipayTradeRefundRequest refundRequest = new AlipayTradeRefundRequest();
        //商品订单号，必填
        String out_trade_no = rr.getOutTradeNo();
        //需要退款的金额，该金额不能大于订单金额(已在做标记时进行判断，此处不会出现大于的情况)，必填
        String refund_amount = rr.getRefundAmount();
        //标识一次退款请求，部分退款则必传
        String out_request_no = rr.getOutRequestNo();

        //设置请求体，字符转义很重要
        refundRequest.setBizContent("{" + "\"out_trade_no\":\"" + out_trade_no + "\","
                + "\"refund_amount\":\"" + refund_amount + "\","
                + "\"out_request_no\":\"" + out_request_no + "\"}");

        //请求
        AlipayTradeRefundResponse result = null;

        try{
            //发起请求，返回结果
            result = alipayClient.execute(refundRequest);
        }catch (AlipayApiException e){
            e.printStackTrace();
        }

        if (result.isSuccess()) {
            //退款成功
            String refundSuccessResult = newBeeMallOrderService.refundSuccess(orderNo);
            if(!ServiceResultEnum.SUCCESS.getResult().equals(refundSuccessResult)){
                return ResultGenerator.genFailResult(refundSuccessResult);
            }
            return ResultGenerator.genSuccessResult();
        } else {
            return ResultGenerator.genFailResult("退款失败,支付宝异常");
        }
    }

    @RepeatSubmit
    @PutMapping("/orders/{orderNo}/finish")
    @ResponseBody
    public Result finishOrder(@PathVariable("orderNo") String orderNo, HttpSession httpSession) {
        NewBeeMallUserVO user = (NewBeeMallUserVO) httpSession.getAttribute(Constants.MALL_USER_SESSION_KEY);
        String finishOrderResult = newBeeMallOrderService.finishOrder(orderNo, user.getUserId());
        if (ServiceResultEnum.SUCCESS.getResult().equals(finishOrderResult)) {
            return ResultGenerator.genSuccessResult();
        } else {
            return ResultGenerator.genFailResult(finishOrderResult);
        }
    }

    /**
     * 判断订单关联用户id和当前登陆用户是否一致
     *
     * @param orderNo 订单编号
     * @param userId  用户ID
     * @return 验证成功后返回订单对象
     */
    private NewBeeMallOrder judgeOrderUserId(String orderNo, Long userId) {
        NewBeeMallOrder newBeeMallOrder = newBeeMallOrderService.getNewBeeMallOrderByOrderNo(orderNo);
        // 判断订单userId
        if (newBeeMallOrder == null || !newBeeMallOrder.getUserId().equals(userId)) {
            throw new NewBeeMallException("当前订单用户异常");
        }
        return newBeeMallOrder;
    }
}
