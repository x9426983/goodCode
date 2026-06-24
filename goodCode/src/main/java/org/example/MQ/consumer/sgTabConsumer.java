package org.example.MQ.consumer;

import com.dianping.cat.Cat;
import com.meituan.mafka.client.consumer.ConsumeStatus;
import com.sankuai.meituan.waimai.rank.compress.util.LogCompressToolV2;
import com.sankuai.meituan.waimai.rank.msg.processor.constants.CATConstant;
import com.sankuai.meituan.waimai.rank.msg.processor.constants.Constant;
import com.sankuai.meituan.waimai.rank.msg.processor.dto.MessageWrapper;
import com.sankuai.meituan.waimai.rank.msg.processor.service.IMessageConsumer;
import com.sankuai.meituan.waimai.rank.msg.processor.util.RankJobThreadPool;
import com.sankuai.meituan.waimai.rank.util.spring.SpringContextUtil;
import com.sankuai.meituan.waimai.rec.hub.client.RecHubService;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SkuRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: fangyinrui
 * @Date: 2026/06/16
 * @Description:
 */
@Service("sgTabConsumer")
@Slf4j
public class sgTabConsumer implements IMessageConsumer {


    @Override
    public ConsumeStatus consume(MessageWrapper messageWrapper) {
        try {
            if (messageWrapper == null || StringUtils.isBlank(messageWrapper.getRequestStr())) {
                log.error("step=[consume] what=[invalid_message_wrapper]");
                return ConsumeStatus.CONSUME_SUCCESS;
            }

            SkuRequest skuRequest = new SkuRequest();
            try {
                LogCompressToolV2.thriftUncompress(skuRequest, messageWrapper.getRequestStr(), false);
            } catch (Exception e) {
                log.error("step=[consume] what=[json_deserialization_failed] detail=[{}]", e.getMessage(), e);
                return ConsumeStatus.CONSUME_SUCCESS;
            }

            Map<String, String> extraParams = skuRequest.getExtraParams();
            if (extraParams == null) {
                extraParams = new HashMap<>();
                skuRequest.setExtraParams(extraParams);
            }

            extraParams.put(Constant.RESET_FLAG, Constant.RESET_FLAG_VALUE);    //重放流量标记

            RecHubService.Iface service = SpringContextUtil.getTypedBean(RecHubService.Iface.class, "recHubService");

            final SkuRequest finalSkuRequest = skuRequest;
            RankJobThreadPool.submit(() -> invokeRankItemAsync(service, finalSkuRequest));

        } catch (Exception e) {
            log.error("step=[consume] what=[exception]", e);
        }


        return ConsumeStatus.CONSUME_SUCCESS;
    }


    private void invokeRankItemAsync(RecHubService.Iface service, SkuRequest skuRequest) {
        try {
            service.rankSku(skuRequest);
        } catch (TException e) {
            // Thrift 异常：包括网络超时、服务异常等
            String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (errorMsg.contains("timeout") || errorMsg.contains("Timeout")) {
                // 超时异常：下游服务可能已接收请求并继续执行
                Cat.logBatchEvent(CATConstant.SG_RESET, "TIMEOUT", 1, 1);
            } else {
                log.error("step=[rankSku] what=[exception], detail={}", errorMsg, e);
                Cat.logBatchEvent(CATConstant.SG_RESET, "EXCEPTION", 1, 1);
            }
        } catch (Exception e) {
            Cat.logBatchEvent(CATConstant.SG_RESET, "EXCEPTION", 1, 1);
        }
    }

}
