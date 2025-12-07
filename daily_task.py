import akshare as ak
import pandas as pd
import numpy as np
import json
import time
import random
from datetime import datetime

# --- 配置区域 ---
# 限制计算的板块数量。建议先设置 50 测试，稳定后改为 300 或 500
CONCEPT_LIMIT = 50 
OUTPUT_FILE = "market_data_v2.json"

def get_market_concepts_safe():
    """安全获取板块列表，带重试机制"""
    print("Step 1: 正在获取板块列表...")
    
    for i in range(3): # 重试3次
        try:
            # 增加随机延时，防止被封
            time.sleep(random.uniform(1, 3))
            
            df = ak.stock_board_concept_name_em()
            
            # 数据校验
            if df is None or df.empty:
                raise ValueError("获取到的数据为空")
                
            count = len(df)
            print(f"✅ 成功获取 {count} 个概念板块 (本次运行截取前 {CONCEPT_LIMIT} 个)")
            return df[['板块名称', '板块代码']].head(CONCEPT_LIMIT)
            
        except Exception as e:
            print(f"⚠️ 第 {i+1} 次尝试失败: {e}")
            time.sleep(5) # 失败后多等几秒

    print("❌ 多次尝试获取板块列表均失败，程序终止。")
    return pd.DataFrame()

def calculate_indicators_safe(board_name):
    """安全计算单个板块指标，带重试机制"""
    for i in range(3): # 对每个板块重试3次
        try:
            # 随机延时
            time.sleep(random.uniform(0.5, 1.5))
            
            # 获取日线数据 (前复权)
            df = ak.stock_board_concept_hist_em(symbol=board_name, period="daily", adjust="qfq")
            
            if df is None or df.empty or len(df) < 60:
                # 如果数据太少，就不重试了，可能是新板块
                return None

            # --- 核心算法 (OBV + VRPS) ---
            
            # 1. OBV 潜伏分 (60日)
            df_60 = df.tail(60).copy()
            df_60['change'] = df_60['收盘'].diff()
            # 涨为1，跌为-1，平为0
            df_60['direction'] = np.where(df_60['change'] > 0, 1, -1)
            df_60['direction'] = np.where(df_60['change'] == 0, 0, df_60['direction'])
            # 计算 OBV 净增量
            obv_net_change = (df_60['成交量'] * df_60['direction']).sum()
            price_change_60 = (df_60['收盘'].iloc[-1] - df_60['收盘'].iloc[0]) / df_60['收盘'].iloc[0]

            # 2. VRPS 强度 (20日)
            df_20 = df.tail(20).copy()
            price_change_20 = (df_20['收盘'].iloc[-1] - df_20['收盘'].iloc[0]) / df_20['收盘'].iloc[0]
            # 量比：今日成交量 / (20日均量 + 1)
            vol_ratio = df_20['成交量'].iloc[-1] / (df_20['成交量'].mean() + 1)

            return {
                "name": board_name,
                "pct_20d": price_change_20,
                "vol_ratio": vol_ratio,
                "obv_change": obv_net_change,
                "pct_60d": price_change_60
            }
            
        except Exception as e:
            # 只有网络类错误才值得打印，数据类错误直接跳过
            if i == 2: # 最后一次尝试还失败
                print(f"❌ [跳过] {board_name}: {e}")
            time.sleep(2)
            continue
            
    return None

def main():
    # 1. 获取列表
    concepts_df = get_market_concepts_safe()
    
    if concepts_df.empty:
        print("❌ 无法开始计算，因为没有板块列表。")
        return

    print(f"Step 2: 开始逐个计算 {len(concepts_df)} 个板块...")
    
    results = []
    success_count = 0
    
    for index, row in concepts_df.iterrows():
        name = row['板块名称']
        res = calculate_indicators_safe(name)
        if res:
            results.append(res)
            success_count += 1
            
        # 简单的进度显示
        if index > 0 and index % 10 == 0:
            print(f"进度: {index}/{len(concepts_df)} | 成功获取: {success_count} 个")

    if not results:
        print("❌ 警告：所有板块均计算失败，未生成数据。")
        return

    print("Step 3: 计算全市场排名与宏观指标...")

    # --- 数据处理与排名 ---
    df_res = pd.DataFrame(results)
    
    # 1. VRPS 计算
    df_res['rank_rps'] = df_res['pct_20d'].rank(pct=True) * 100
    df_res['rank_vol'] = df_res['vol_ratio'].rank(pct=True) * 100
    df_res['vrps'] = df_res['rank_rps'] * 0.7 + df_res['rank_vol'] * 0.3
    
    # 2. 潜伏分 计算
    df_res['rank_obv'] = df_res['obv_change'].rank(pct=True) * 100
    df_res['rank_price_60'] = df_res['pct_60d'].rank(pct=True) * 100
    df_res['stealth_score'] = df_res['rank_obv'] - df_res['rank_price_60']

    # 3. 宏观天气
    hot_ratio = (len(df_res[df_res['vrps'] > 80]) / len(df_res)) * 100
    if hot_ratio > 15:
        weather = "sunny"
    elif hot_ratio < 5:
        weather = "rainy"
    else:
        weather = "cloudy"
    
    # 4. 导出 JSON
    final_data = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "macro": {
            "weather": weather,
            "hot_ratio": round(hot_ratio, 2)
        },
        "data": df_res.sort_values('vrps', ascending=False).to_dict(orient='records')
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)
        
    print(f"✅✅✅ 任务圆满完成！数据已保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
