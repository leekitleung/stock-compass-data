import akshare as ak
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime

# 配置：为了演示速度，先取前100个活跃板块，稳定后可调大到 500
CONCEPT_LIMIT = 100
OUTPUT_FILE = "market_data_v2.json"

def get_market_concepts():
    print("Step 1: 获取板块列表...")
    try:
        df = ak.stock_board_concept_name_em()
        return df[['板块名称', '板块代码']].head(CONCEPT_LIMIT)
    except Exception as e:
        print(f"获取板块失败: {e}")
        return pd.DataFrame()

def calculate_indicators(board_name):
    try:
        # 获取日线数据 (前复权)
        df = ak.stock_board_concept_hist_em(symbol=board_name, period="daily", adjust="qfq")
        
        if df.empty or len(df) < 60:
            return None

        # --- 核心算法 ---
        # 1. OBV 潜伏分 (60日)
        df_60 = df.tail(60).copy()
        df_60['change'] = df_60['收盘'].diff()
        # 涨为1，跌为-1，平为0
        df_60['direction'] = np.where(df_60['change'] > 0, 1, -1)
        df_60['direction'] = np.where(df_60['change'] == 0, 0, df_60['direction'])
        # 真正的主力资金代理变量
        obv_net_change = (df_60['成交量'] * df_60['direction']).sum()
        price_change_60 = (df_60['收盘'].iloc[-1] - df_60['收盘'].iloc[0]) / df_60['收盘'].iloc[0]

        # 2. VRPS 强度 (20日)
        df_20 = df.tail(20).copy()
        price_change_20 = (df_20['收盘'].iloc[-1] - df_20['收盘'].iloc[0]) / df_20['收盘'].iloc[0]
        # 量比：今日成交量 / 20日均量
        vol_ratio = df_20['成交量'].iloc[-1] / (df_20['成交量'].mean() + 1) # +1防除零

        return {
            "name": board_name,
            "pct_20d": price_change_20,
            "vol_ratio": vol_ratio,
            "obv_change": obv_net_change,
            "pct_60d": price_change_60
        }
    except:
        return None

def main():
    concepts_df = get_market_concepts()
    print(f"开始计算 {len(concepts_df)} 个板块指标...")
    
    results = []
    for index, row in concepts_df.iterrows():
        name = row['板块名称']
        res = calculate_indicators(name)
        if res:
            results.append(res)
        # 稍微暂停一下，防止请求过快
        if index % 10 == 0:
            time.sleep(0.5)

    if not results:
        print("无数据生成")
        return

    # --- 全市场排名归一化 ---
    df_res = pd.DataFrame(results)
    
    # 1. 计算 VRPS (短期强度)
    df_res['rank_rps'] = df_res['pct_20d'].rank(pct=True) * 100
    df_res['rank_vol'] = df_res['vol_ratio'].rank(pct=True) * 100
    df_res['vrps'] = df_res['rank_rps'] * 0.7 + df_res['rank_vol'] * 0.3
    
    # 2. 计算潜伏分 (长期背离)
    df_res['rank_obv'] = df_res['obv_change'].rank(pct=True) * 100
    df_res['rank_price_60'] = df_res['pct_60d'].rank(pct=True) * 100
    # 核心逻辑：资金排名前，价格排名后 = 潜伏
    df_res['stealth_score'] = df_res['rank_obv'] - df_res['rank_price_60']

    # 3. 宏观天气判断
    # 强势板块(VRPS>80)占比超过15%认为行情好
    hot_ratio = (len(df_res[df_res['vrps'] > 80]) / len(df_res)) * 100
    weather = "sunny" if hot_ratio > 15 else ("rainy" if hot_ratio < 5 else "cloudy")
    
    # 输出 JSON
    final_data = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "macro": {"weather": weather, "hot_ratio": round(hot_ratio, 2)},
        "data": df_res.sort_values('vrps', ascending=False).to_dict(orient='records')
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)
    print("计算完成，JSON已生成")

if __name__ == "__main__":
    main()
