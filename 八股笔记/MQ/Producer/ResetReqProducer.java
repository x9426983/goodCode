package org.example.MQ.Producer;

import com.alibaba.fastjson.JSONObject;
import com.dianping.cat.Cat;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.meituan.dorado.serialize.thrift.ThriftSerializer;
import com.meituan.jmonitor.LOG;
import com.meituan.mafka.client.producer.AsyncProducerResult;
import com.meituan.mafka.client.producer.FutureCallback;
import com.meituan.mafka.client.producer.IProducerProcessor;
import com.meituan.mtrace.Tracer;
import com.meituan.mtrace.thread.pool.ExecutorServiceTraceWrapper;
import com.sankuai.meituan.waimai.rank.compress.util.LogCompressToolV2;
import com.sankuai.meituan.waimai.rec.hub.client.coupon.CouponRequest;
import com.sankuai.meituan.waimai.rec.hub.client.med.MedRequest;
import com.sankuai.meituan.waimai.rec.hub.client.mix.MixRequest;
import com.sankuai.meituan.waimai.rec.hub.client.phfmix.PhfMixRequest;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SkuRequest;
import com.sankuai.meituan.waimai.rec.hub.constants.ModelResetConstants;
import com.sankuai.meituan.waimai.rec.hub.constants.PhfSpuConstants;
import com.sankuai.meituan.waimai.rec.hub.context.*;
import com.sankuai.meituan.waimai.rec.hub.domain.MessageWrapper;
import com.sankuai.meituan.waimai.rec.hub.operator.parser.convert.ModelResetTimeConvert;
import com.sankuai.meituan.waimai.rec.hub.util.HashUtil;
import com.sankuai.meituan.waimai.rec.hub.util.JsonUtil;
import com.sankuai.meituan.waimai.rec.hub.util.ThriftJsonSerializerUtil;
import com.sankuai.meituan.waimai.rec.hub.util.WmBListUtil;
import com.sankuai.worldtree.debugservice.api.domain.message.MessageSerializer;
import com.sankuai.worldtree.debugservice.api.util.GsonUtil;
import com.sankuai.worldtree.debugservice.api.util.JacksonCustomizedUtil;
import com.sankuai.worldtree.debugservice.api.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: fangyinrui
 * @Date: 2026/04/13
 * @Description:
 */
@Slf4j
@Component
public class ResetReqProducer {


    @Autowired
    private  IProducerProcessor producer;

    private static volatile boolean isInit = false;

    private static ExecutorService executorService = null;

    private static void init(){
        if(isInit){
            return;
        }

        synchronized (ResetReqProducer.class){
            if(isInit){
                return;
            }

            executorService = new ExecutorServiceTraceWrapper(
                    new ThreadPoolExecutor(8, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(128),
                            new ThreadFactoryBuilder().setNameFormat("ResetReqProducerExecutor").build(), new ThreadPoolExecutor.DiscardPolicy())
            );

            isInit = true;
        }

    }


    public void sendResetReq(BaseContext context) {
        if(context==null){
            return;
        }

//        MessageWrapper messageWrapper = null;
        com.sankuai.meituan.waimai.rec.hub.domain.MessageWrapper messageWrapper = new MessageWrapper();
        try {

            if (context instanceof PhfMixContext) {
                PhfMixContext phfMixContext = (PhfMixContext) context;
                PhfMixRequest originalRequest = phfMixContext.getPhfMixRequest();

                String requestStr = LogCompressToolV2.thriftCompress(originalRequest, false);

                messageWrapper.setClassName(PhfMixRequest.class.getName());
                messageWrapper.setRequestStr(requestStr);
//                messageWrapper = MessageSerializer.serialize(originalRequest);

                sendMsg(messageWrapper, context.getScene());
            } else if (context instanceof WmBListContext) {
                WmBListContext bListContext = (WmBListContext) context;

                //b版额外判断推荐 tab 请求
                if(!validBListRequest(bListContext)){
                    return;
                }

                MixRequest originalRequest = bListContext.getMixRequest();


                String requestStr = LogCompressToolV2.thriftCompress(originalRequest, false);
                messageWrapper.setClassName(MixRequest.class.getName());
                messageWrapper.setRequestStr(requestStr);

                sendMsg(messageWrapper, context.getScene());
            } else if (context instanceof MedContext) {
                MedContext medContext = (MedContext) context;
                MedRequest originalRequest = medContext.getMedRequest();

                String requestStr = LogCompressToolV2.thriftCompress(originalRequest, false);
                messageWrapper.setClassName(MedRequest.class.getName());
                messageWrapper.setRequestStr(requestStr);

                sendMsg(messageWrapper, context.getScene());
            } else if (context instanceof SkuContext) {
                SkuContext skuContext = (SkuContext) context;
                SkuRequest originalRequest = skuContext.getSkuRequest();

                String requestStr = LogCompressToolV2.thriftCompress(originalRequest, false);
                messageWrapper.setClassName(SkuRequest.class.getName());
                messageWrapper.setRequestStr(requestStr);

                sendMsg(messageWrapper, context.getScene());
            } else {

            }


        }catch (Exception e){
            log.error("send reset req error, context: {}", context.getScene(), e);
        }
    }



    private void sendMsg(MessageWrapper messageWrapper, String scene){

        try {
//            String message = JacksonCustomizedUtil.serialize(messageWrapper);
            String message = JsonUtils.toStr(messageWrapper);

            executorService.submit(() -> producer.sendAsyncMessage(message, new FutureCallback(){

                @Override
                public void onSuccess(AsyncProducerResult asyncProducerResult) {
                    Cat.logBatchEvent(PhfSpuConstants.RESET_REQ, scene, 1, 0);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    Cat.logBatchEvent(PhfSpuConstants.RESET_REQ, scene, 1, 1);
                    log.error("send reset req failure", throwable);
                }
            }));
        }catch (Exception e){
            log.error("send msg error, messageWrapper: {}", messageWrapper.getClassName(), e);
        }


    }

    private boolean validBListRequest(WmBListContext bListContext) {

        MixRequest mixRequest = bListContext.getMixRequest();
        //tabcode 不为 null，则是筛选 tab
        if(mixRequest.getBizReqData() != null && StringUtils.isNotEmpty(mixRequest.getBizReqData().getTabCode())){
            return false;
        }

        //屏蔽其他入口（tab 为 null 的情况）
        if(WmBListUtil.isClickMixWindow(mixRequest.getExtraParams())
                || WmBListUtil.isClickMultipleMeal(bListContext)
                || WmBListUtil.isSupplyCheckMultipleMeal(mixRequest.getExtraParams())){
            return false;
        }

        return true;
    }


    public static boolean enableResetReq(BaseContext context) {
        if (context.isResetFlag() || Tracer.isTest() || context.getOffset()!=0) {
            return false;
        }

        if (!isInit) {
            init();
        }

        List<ModelResetTimeConvert.TimeScope> timeScope = ModelResetConstants.getModelResetTimeByScene(context.getScene());
        // 检查当前时间是否在配置的时间段内
        if (!isInTimeScope(timeScope)) {
            return false;
        }

        int hashVal = HashUtil.hashByStr(ModelResetConstants.hashSeed, context.getBizTraceId());
        if (ModelResetConstants.getModelResetRatioByScene(context.getScene()) == 0
                || hashVal <= 0 || hashVal > ModelResetConstants.getModelResetRatioByScene(context.getScene())) {
            return false;
        }

        return true;
    }

    /**
     * 判断当前时间是否在任一时间段内
     *
     * @param timeScopeList 时间段列表
     * @return 如果当前时间符合任一时间段则返回true，否则返回false
     */
    public static boolean isInTimeScope(List<ModelResetTimeConvert.TimeScope> timeScopeList) {
        if (timeScopeList == null || timeScopeList.isEmpty()) {
            return false;
        }

        LocalTime currentTime = LocalTime.now();

        for (ModelResetTimeConvert.TimeScope timeScope : timeScopeList) {
            if (isInSingleTimeScope(currentTime, timeScope)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断当前时间是否在单个时间段内
     * 支持跨天的时间段（如 20:00 - 10:00）
     */
    private static boolean isInSingleTimeScope(LocalTime currentTime, ModelResetTimeConvert.TimeScope timeScope) {
        try {
            LocalTime startTime = LocalTime.parse(timeScope.getStartTime(), DateTimeFormatter.ofPattern("HH:mm"));
            LocalTime endTime = LocalTime.parse(timeScope.getEndTime(), DateTimeFormatter.ofPattern("HH:mm"));

            if (startTime.isBefore(endTime)) {
                // 普通情况：不跨天（如 08:00 - 18:00）
                return !currentTime.isBefore(startTime) && currentTime.isBefore(endTime);
            } else if (startTime.isAfter(endTime)) {
                // 跨天情况（如 20:00 - 10:00）
                return currentTime.isAfter(startTime) || currentTime.isBefore(endTime);
            } else {
                // startTime 等于 endTime，整天都符合
                return true;
            }
        } catch (Exception e) {
            // 时间格式解析失败
            return false;
        }
    }
}
