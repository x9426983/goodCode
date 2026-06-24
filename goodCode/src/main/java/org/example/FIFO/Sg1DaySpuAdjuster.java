package org.example.FIFO;

import com.dianping.cat.Cat;
import com.sankuai.meituan.waimai.recsys.engineering.itemcf.ParamUtil;
import com.sankuai.meituan.waimai.worldtree.graph.annotation.FieldMap;
import com.sankuai.meituan.waimai.worldtree.operator.core.ExecutionEnvironment;
import com.sankuai.meituan.waimai.worldtree.operator.core.SyncInput2Op;
import com.sankuai.meituan.waimai.worldtree.operator.core.annotation.Input;
import com.sankuai.meituan.waimai.worldtree.operator.core.annotation.OperatorDoc;
import com.sankuai.meituan.waimai.worldtree.operator.core.annotation.Output;
import com.sankuai.meituan.waimai.worldtree.operator.util.OutputUtil;
import com.sankuai.meituan.waimai.worldtree.operator.util.TableUtil;
import com.sankuai.meituan.waimai.worldtree.table.IntArray;
import com.sankuai.meituan.waimai.worldtree.table.Table;
import com.sankuai.meituan.waimai.worldtree.table.data.LongField;
import com.sankuai.wmdrecsys.river.server.constants.CatKeyConst;
import com.sankuai.wmdrecsys.river.server.context.ContextBase;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 闪购1天内相关行为（加购/提单/点击）的spu打压算子。
 */
@OperatorDoc(
        desc = "闪购1天内有行为的spu展示位次后移，新位次 = 原位次 + demoteStep",
        input = {
                @Input(type = Table.class, desc = "商品表"),
                @Input(type = Map.class, desc = "Map<Long, Integer> ")
        },
        output = {
                @Output(type = Table.class, desc = "位次调整后的商品表")
        }
)
public class Sg1DaySpuAdjuster extends SyncInput2Op<Table, Map<Long, Integer>> {

    private static final Logger logger = LoggerFactory.getLogger(Sg1DaySpuAdjuster.class);

    private static final String PARAM_DEMOTE_STEP = "demoteStep";
    private static final int DEFAULT_DEMOTE_STEP = 6;

    private Meta meta;

    @FieldMap
    private static class Meta {
        private LongField spuId;
    }

    @Override
    public Object[] compute(ExecutionEnvironment env, Table table, Map<Long, Integer> spuIdMap) throws Exception {
        if (TableUtil.isEmpty(table) || MapUtils.isEmpty(spuIdMap)) {
            return OutputUtil.wrapperObjectArray(table);
        }
        ContextBase context = (ContextBase) env;

        int step = ParamUtil.getIntegerParam(PARAM_DEMOTE_STEP, DEFAULT_DEMOTE_STEP, runParams);
        Set<Long> spuIdSet = spuIdMap.keySet();

        // FIFO 队列：存放待插入的命中项
        // 每个元素 int[]{row, slotsLeft}，slotsLeft 表示还需放行多少个正常项后才插入
        Deque<int[]> queue = new ArrayDeque<>();
        List<Integer> result = new ArrayList<>();
        AtomicInteger demotedCount = new AtomicInteger();

        AtomicInteger count = new AtomicInteger();

        table.foreach(row -> {
            Long spuId = meta.spuId.get(table, row);
            if (spuIdSet.contains(spuId)) {
                queue.addLast(new int[]{row, step});
                demotedCount.incrementAndGet();
                count.incrementAndGet();
            } else {
                // 正常项：立即加入结果
                result.add(row);
                // 对队列中所有项计数 -1；归零的按入队顺序（FIFO）依次插入结果
                if (!queue.isEmpty()) {
                    Iterator<int[]> it = queue.iterator();
                    List<int[]> matured = new ArrayList<>();
                    while (it.hasNext()) {
                        int[] entry = it.next();
                        if (--entry[1] <= 0) {
                            matured.add(entry);
                            it.remove();
                        }
                    }
                    for (int[] m : matured) {
                        result.add(m[0]);
                    }
                }
            }
        });

        if (demotedCount.get() == 0) {
            return OutputUtil.wrapperObjectArray(table);
        }

        // 末尾兜底：正常项不足 step 个的命中项按入队顺序追加到末尾
        for (int[] entry : queue) {
            result.add(entry[0]);
        }

        // 将新排列写回 docIndex
        int size = result.size();
        IntArray newDocIndex = new IntArray(size);
        for (int row : result) {
            newDocIndex.append(row);
        }
        table.updateDocIndex(newDocIndex);

        Cat.logBatchEvent(CatKeyConst.PRODUCT_CAT,context.getGraphScene()+"_Sg1DaySpuAdjuster",size,demotedCount.get());

        return OutputUtil.wrapperObjectArray(table);
    }
}

