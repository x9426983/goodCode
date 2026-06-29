package org.example.MQ.consumer;

import com.sankuai.meituan.waimai.d.traffic.mix_rec.client.MROutletsRequest;
import com.sankuai.meituan.waimai.rank.msg.processor.dto.MessageWrapper;
import com.sankuai.meituan.waimai.rec.hub.client.coupon.CouponRequest;
import com.sankuai.meituan.waimai.rec.hub.client.med.MedRequest;
import com.sankuai.meituan.waimai.rec.hub.client.mix.MixRequest;
import com.sankuai.meituan.waimai.rec.hub.client.phfmix.PhfMixRequest;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SkuRequest;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @Auther: fangyinrui
 * @Date: 2026/04/14
 * @Description:
 */
@Component
public class MessageConsumerFactory {

    @Resource(name = "phfFeedConsumer")
    private IMessageConsumer phfFeedConsumer;

    @Resource(name = "sqsFeedConsumer")
    private IMessageConsumer sqsFeedConsumer;

    @Resource(name = "blistConsumer")
    private IMessageConsumer blistConsumer;

    @Resource(name = "medTabConsumer")
    private IMessageConsumer medTabConsumer;

    @Resource(name = "sgTabConsumer")
    private IMessageConsumer sgTabConsumer;



    public IMessageConsumer getConsumer(MessageWrapper messageWrapper) {

        if (messageWrapper.getClassName().equals(PhfMixRequest.class.getName())) {
            return phfFeedConsumer;
        } else if(messageWrapper.getClassName().equals(MROutletsRequest.class.getName())){
            return sqsFeedConsumer;
        } else if(messageWrapper.getClassName().equals(MixRequest.class.getName())){
            return blistConsumer;
        } else if (messageWrapper.getClassName().equals(MedRequest.class.getName())) {
            return medTabConsumer;
        } else if (messageWrapper.getClassName().equals(SkuRequest.class.getName())) {
            return sgTabConsumer;
        }

        return null;
    }

}
