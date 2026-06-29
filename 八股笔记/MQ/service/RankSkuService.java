package org.example.MQ.service;


import com.dianping.cat.Cat;
import com.dianping.rhino.onelimiter.LimitResult;
import com.google.gson.Gson;
import com.meituan.mtrace.Tracer;
import com.sankuai.meituan.waimai.d.debug.trace.sdk.util.SgRecTraceInfoUtil;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SeniorScene;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SkuInfo;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SkuRequest;
import com.sankuai.meituan.waimai.rec.hub.client.sku.SkuResponse;
import com.sankuai.meituan.waimai.rec.hub.constants.ToteConstants;
import com.sankuai.meituan.waimai.rec.hub.constants.WmSpuConstants;
import com.sankuai.meituan.waimai.rec.hub.context.SkuContext;
import com.sankuai.meituan.waimai.rec.hub.service.mafka.ResetReqProducer;
import com.sankuai.meituan.waimai.rec.hub.strategy.graph.GraphStrategy;
import com.sankuai.meituan.waimai.rec.hub.util.RhinoLimiterUtil;
import com.sankuai.meituan.waimai.rec.hub.util.ToteDebugUtil;
import com.sankuai.meituan.waimai.worldtree.graph.core.TaskFlowFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RankSkuService {

    @Autowired
    private TaskFlowFactory taskFlowFactory;

    @Autowired
    private ResetReqProducer resetReqProducer;

    private Gson gson = new Gson();


    public SkuResponse rankSkuList(SkuRequest skuRequest, String interfaceMethod) {
        Map<String, String> limitParams = buildRhinoLimitParam(skuRequest);
        LimitResult result = RhinoLimiterUtil.getLimitResult(interfaceMethod, limitParams);
        if (!result.isPass()) {
            return degrade(skuRequest);
        }

        SkuContext context = SkuContext.buildSkuContext(skuRequest, interfaceMethod);

        if (ToteConstants.SG_BUSINESS_SCENE.contains(context.getScene())) {
            boolean isNeedDebug = false;
            if(skuRequest.getExtraParams() != null){
                isNeedDebug = Boolean.parseBoolean(skuRequest.getExtraParams().getOrDefault("isToteDebug", "false"));
            }
            SgRecTraceInfoUtil.setNeedTraceInfo(isNeedDebug, context.getUuid());
        }
        if (SgRecTraceInfoUtil.needTrace()) {
            SgRecTraceInfoUtil.setSubModuleName(context.getScene());
            SgRecTraceInfoUtil.addRelationshipInfoSource(context.getUuid(), Tracer.id(), "sg_rec_" + context.getScene());
            SgRecTraceInfoUtil.addObjSummaryInfo("request", gson.toJson(skuRequest));
        }

        GraphStrategy graphStrategy = GraphStrategy.buildGraph(context, taskFlowFactory);
        graphStrategy.start();
        validateResponse(context);
        catRankSkuResponse(context, skuRequest);

        // 记录回放请求
        if(ResetReqProducer.enableResetReq(context)){
            resetReqProducer.sendResetReq(context);
        }

        if (SgRecTraceInfoUtil.needTrace()) {
            ToteDebugUtil.toteExpSummaryInfo(context);
            ToteDebugUtil.totePageSummaryInfo(context);
        }
        return context.getSkuResponse();
    }
    protected void catRankSkuResponse(SkuContext context, SkuRequest skuRequest) {
        SkuResponse response = context.getSkuResponse();
        int responseSize = 0;
        if (response != null && CollectionUtils.isNotEmpty(response.getSkuInfoList())) {
            responseSize = response.getSkuInfoListSize();
        }
        Cat.newCompletedTransactionWithDuration(WmSpuConstants.BIZ_SG_PRODUCT_NUM, "rankSku." + context.getFinalScene(), responseSize);
        Cat.logBatchEvent(WmSpuConstants.BIZ_SG_PRODUCT_EMPTY, "rankSku." + context.getFinalScene(), 1, responseSize == 0 ? 1 : 0);
    }

    protected void validateResponse(SkuContext context) {
        SkuResponse response = context.getSkuResponse();
        if (!response.isSetSkuInfoList()) {
            response.setSkuInfoList(new ArrayList<>());
        }
        if (!response.isSetIsEndPage()) {
            response.setIsEndPage(true);
        }
    }

    private Map<String, String> buildRhinoLimitParam(SkuRequest skuRequest) {
        Map<String, String> params = new HashMap<>();
        params.put("scene", skuRequest.getSceneToken().getSeniorScene().name());
        return params;
    }

    private SkuResponse degrade(SkuRequest skuRequest) {
        SeniorScene seniorScene = skuRequest.getSceneToken().getSeniorScene();
        if (seniorScene == null) {
            return degradeEmpty();
        }
        if (seniorScene == SeniorScene.SG_INDOOR_REC_TAB || seniorScene == SeniorScene.SG_INDOOR_RELATIVE_SKU || seniorScene == SeniorScene.SG_INDOOR_PRODUCT_DETAIL_RELATIVE_SKU) {
            return degradeEmpty();
        } else if (seniorScene == SeniorScene.SG_INDOOR_PHYSICS_TAB) {
            return degradeSpu(skuRequest);
        } else if (seniorScene == SeniorScene.SG_INDOOR_SKU_RANK) {
            return degradeSku(skuRequest);
        } else if (seniorScene == SeniorScene.SG_INDOOR_REC_TAB_MARKETING_PRICE) {
            return degradeSpu(skuRequest);
        }
        return degradeEmpty();
    }

    private SkuResponse degradeEmpty() {
        SkuResponse skuResponse = new SkuResponse();
        skuResponse.setIsEndPage(true);
        skuResponse.setSkuInfoList(new ArrayList<>());
        return skuResponse;
    }

    private SkuResponse degradeSpu(SkuRequest skuRequest) {
        SkuResponse skuResponse = new SkuResponse();
        skuResponse.setIsEndPage(true);
        skuResponse.setSkuInfoList(new ArrayList<>());
        List<Long> poiIdList = skuRequest.getCommonMeta().getPoiIdList();
        if (CollectionUtils.isEmpty(poiIdList)) {
            return skuResponse;
        }
        Long poiId = skuRequest.getCommonMeta().getPoiIdList().get(0);
        if (CollectionUtils.isNotEmpty(skuRequest.getCommonMeta().getItemIdList())) {
            for (Long itemId : skuRequest.getCommonMeta().getItemIdList()) {
                SkuInfo skuInfo = new SkuInfo();
                skuInfo.setPoiId(poiId);
                skuInfo.setSpuId(itemId);
                skuResponse.getSkuInfoList().add(skuInfo);
            }
        }
        return skuResponse;
    }

    private SkuResponse degradeSku(SkuRequest skuRequest) {
        SkuResponse skuResponse = new SkuResponse();
        skuResponse.setIsEndPage(true);
        skuResponse.setSkuInfoList(new ArrayList<>());
        List<Long> poiIdList = skuRequest.getCommonMeta().getPoiIdList();
        if (CollectionUtils.isEmpty(poiIdList)) {
            return skuResponse;
        }
        Long poiId = skuRequest.getCommonMeta().getPoiIdList().get(0);
        if (CollectionUtils.isNotEmpty(skuRequest.getCommonMeta().getItemIdList())) {
            for (Long itemId : skuRequest.getCommonMeta().getItemIdList()) {
                SkuInfo skuInfo = new SkuInfo();
                skuInfo.setPoiId(poiId);
                skuInfo.setSkuId(itemId);
                skuResponse.getSkuInfoList().add(skuInfo);
            }
        }
        return skuResponse;
    }
}
