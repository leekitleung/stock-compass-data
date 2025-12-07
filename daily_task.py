import akshare as ak
import pandas as pd
import numpy as np
import json
import time
import random
import requests
import os
import subprocess
from datetime import datetime

# --- 配置 ---
CONCEPT_LIMIT = 500  # 本地跑IP稳定，可以直接跑500个！
OUTPUT_FILE = "market_data_v2.json"

# --- 自动推送 GitHub ---
def git_push_automatic():
    print("\n🚀 正在自动推送数据到 GitHub...")
    try:
        # 1. git add
        subprocess.run(["git", "add", OUTPUT_FILE], check=True)
        
        # 2. git commit (如果没变化会报错，忽略即可)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        try:
            subprocess.run(["git", "commit", "-m", f"Local update: {timestamp}"], check=True)
        except subprocess.CalledProcessError:
            print("⚠️ 数据无变化，无需提交")
            return

        # 3. git push
        # 注意：你需要确保本地 git 已经配置好 ssh 或 token，可以直接 push
        subprocess.run(["git", "push"], check=True)
        print("✅✅✅ 成功！数据已同步到云端，uTools 插件即将更新。")
        
    except Exception as e:
        print(f"❌ 推送失败: {e}")
        print("请手动执行: git push")

# --- 爬虫核心逻辑 (保持不变，去掉了过度的延时，本地跑可以快点) ---
def get_market_concepts():
    print("Step 1: 获取板块列表...")
    try:
        df = ak.stock_board_concept_name_em()
        return df[['板块名称', '板块代码']].head(CONCEPT_LIMIT)
    except Exception as e:
        print(f"❌ 列表获取失败: {e}")
        return pd.DataFrame()

def calculate_indicators(board_name):
    for i in range(3):
        try:
            # 本地IP很稳，延时可以设短一点 (0.5秒)
            time.sleep(random.uniform(0.2, 0.8))
            
            df = ak.stock_board_concept_hist_em(symbol=board_name, period="daily", adjust="qfq")
            
            if df is None or df.empty or len(df) < 60:
                return None

            # --- 算法 ---
            df_60 = df.tail(60).copy()
            df_60['change'] = df_60['收盘'].diff()
            df_60['direction'] = np.where(df_60['change'] > 0, 1, -1)
            df_60['direction'] = np.where(df_60['change'] == 0, 0, df_60['direction'])
            
            obv_net_change = (df_60['成交量'] * df_60['direction']).sum()
            price_change_60 = (df_60['收盘'].iloc[-1] - df_60['收盘'].iloc[0]) / df_60['收盘'].iloc[0]

            df_20 = df.tail(20).copy()
            price_change_20 = (df_20['收盘'].iloc[-1] - df_20['收盘'].iloc[0]) / df_20['收盘'].iloc[0]
            vol_ratio = df_20['成交量'].iloc[-1] / (df_20['成交量'].mean() + 1)

            return {
                "name": board_name,
                "pct_20d": price_change_20,
                "vol_ratio": vol_ratio,
                "obv_change": obv_net_change,
                "pct_60d": price_change_60
            }
        except Exception as e:
            if i == 2: print(f"❌ [跳过] {board_name}: {e}")
            time.sleep(1)
            continue
    return None

def main():
    concepts_df = get_market_concepts()
    if concepts_df.empty: return

    print(f"Step 2: 本地开始计算 {len(concepts_df)} 个板块...")
    results = []
    
    for index, row in concepts_df.iterrows():
        name = row['板块名称']
        res = calculate_indicators(name)
        if res:
            results.append(res)
        
        # 进度条
        if index % 20 == 0:
            print(f"进度: {index}/{len(concepts_df)}...")

    if not results:
        print("❌ 本地也获取不到数据，请检查网络或VPN。")
        return

    print("Step 3: 生成结果...")
    df_res = pd.DataFrame(results)
    
    # 排名算法
    df_res['rank_rps'] = df_res['pct_20d'].rank(pct=True) * 100
    df_res['rank_vol'] = df_res['vol_ratio'].rank(pct=True) * 100
    df_res['vrps'] = df_res['rank_rps'] * 0.7 + df_res['rank_vol'] * 0.3
    
    df_res['rank_obv'] = df_res['obv_change'].rank(pct=True) * 100
    df_res['rank_price_60'] = df_res['pct_60d'].rank(pct=True) * 100
    df_res['stealth_score'] = df_res['rank_obv'] - df_res['rank_price_60']

    hot_ratio = (len(df_res[df_res['vrps'] > 80]) / len(df_res)) * 100
    weather = "sunny" if hot_ratio > 15 else ("rainy" if hot_ratio < 5 else "cloudy")
    
    final_data = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "macro": {"weather": weather, "hot_ratio": round(hot_ratio, 2)},
        "data": df_res.sort_values('vrps', ascending=False).to_dict(orient='records')
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)
        
    print(f"✅ JSON 生成完毕！")
    
    # --- 触发自动推送 ---
    git_push_automatic()

if __name__ == "__main__":
    main()
