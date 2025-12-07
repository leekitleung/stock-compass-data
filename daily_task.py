import akshare as ak
import pandas as pd
import numpy as np
import json
import time
import random
import requests
from datetime import datetime

# --- 全局配置 ---
# ⚠️ 注意：GitHub Actions 运行时间有限，且容易被封。
# 建议先设置较小的数量 (如 50) 进行测试，稳定后再调大到 300
CONCEPT_LIMIT = 50 
OUTPUT_FILE = "market_data_v2.json"

# --- 核心黑科技：全局伪装 ---
# 修改 requests 的默认 User-Agent，伪装成浏览器
def set_global_proxy():
    old_init = requests.Session.__init__
    def new_init(self, *args, **kwargs):
        old_init(self, *args, **kwargs)
        self.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        })
    requests.Session.__init__ = new_init

# 启用伪装
set_global_proxy()

def get_market_concepts_safe():
    """获取板块列表"""
    print("Step 1: 正在获取板块列表...")
    for i in range(3):
        try:
            # 随机延时
            time.sleep(random.uniform(1, 2))
            df = ak.stock_board_concept_name_em()
            
            if df is not None and not df.empty:
                count = len(df)
                print(f"✅ 成功获取 {count} 个概念板块 (本次截取前 {CONCEPT_LIMIT} 个)")
                return df[['板块名称', '板块代码']].head(CONCEPT_LIMIT)
        except Exception as e:
            print(f"⚠️ 获取列表第 {i+1} 次尝试失败: {e}")
            time.sleep(3)
            
    print("❌ 无法获取板块列表，程序终止。")
    return pd.DataFrame()

def calculate_indicators_safe(board_name):
    """计算单个板块指标"""
    # 这里的重试非常重要，应对 RemoteDisconnected
    for i in range(3): 
        try:
            # 🟢 关键：每次请求前随机歇一会，模拟人类操作
            time.sleep(random.uniform(1.5, 3.5))
            
            # 获取日线数据
            df = ak.stock_board_concept_hist_em(symbol=board_name, period="daily", adjust="qfq")
            
            if df is None or df.empty or len(df) < 60:
                return None

            # --- 算法逻辑 ---
            # 1. OBV 潜伏分 (60日)
            df_60 = df.tail(60).copy()
            df_60['change'] = df_60['收盘'].diff()
            df_60['direction'] = np.where(df_60['change'] > 0, 1, -1)
            df_60['direction'] = np.where(df_60['change'] == 0, 0, df_60['direction'])
            obv_net_change = (df_60['成交量'] * df_60['direction']).sum()
            price_change_60 = (df_60['收盘'].iloc[-1] - df_60['收盘'].iloc[0]) / df_60['收盘'].iloc[0]

            # 2. VRPS 强度 (20日)
            df_20 = df.tail(20).copy()
            price_change_20 = (df_20['收盘'].iloc[-1] - df_20['收盘'].iloc[0]) / df_20['收盘'].iloc[0]
            # +1 防止除零
            vol_ratio = df_20['成交量'].iloc[-1] / (df_20['成交量'].mean() + 1)

            return {
                "name": board_name,
                "pct_20d": price_change_20,
                "vol_ratio": vol_ratio,
                "obv_change": obv_net_change,
                "pct_60d": price_change_60
            }
            
        except Exception as e:
            # 如果是连接被断开，休息久一点再试
            if "RemoteDisconnected" in str(e) or "Connection aborted" in str(e):
                # print(f"⚠️ 网络波动 {board_name}, 等待重试...")
                time.sleep(5)
            elif i == 2: # 最后一次还失败，才打印错误
                print(f"❌ [跳过] {board_name}: {e}")
            continue
            
    return None

def main():
    # 1. 获取列表
    concepts_df = get_market_concepts_safe()
    if concepts_df.empty:
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
            
        # 进度提示
        if index > 0 and index % 10 == 0:
            print(f"进度: {index}/{len(concepts_df)} | 成功: {success_count}")

    if not results:
        print("❌ 警告：所有板块均计算失败 (可能是IP被彻底封锁)。")
        # 这里不return，让后续流程生成一个空的json，避免Git报错，或者你可以选择直接退出
        # 为了调试，我们继续往下走，生成一个带错误信息的JSON

    print("Step 3: 计算排名与导出...")

    # --- 排名与保存 ---
    if results:
        df_res = pd.DataFrame(results)
        
        # VRPS
        df_res['rank_rps'] = df_res['pct_20d'].rank(pct=True) * 100
        df_res['rank_vol'] = df_res['vol_ratio'].rank(pct=True) * 100
        df_res['vrps'] = df_res['rank_rps'] * 0.7 + df_res['rank_vol'] * 0.3
        
        # 潜伏分
        df_res['rank_obv'] = df_res['obv_change'].rank(pct=True) * 100
        df_res['rank_price_60'] = df_res['pct_60d'].rank(pct=True) * 100
        df_res['stealth_score'] = df_res['rank_obv'] - df_res['rank_price_60']

        # 宏观天气
        hot_ratio = (len(df_res[df_res['vrps'] > 80]) / len(df_res)) * 100
        weather = "sunny" if hot_ratio > 15 else ("rainy" if hot_ratio < 5 else "cloudy")
        
        data_list = df_res.sort_values('vrps', ascending=False).to_dict(orient='records')
    else:
        # 兜底数据，防止前端白屏
        hot_ratio = 0
        weather = "rainy"
        data_list = []

    final_data = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "macro": {
            "weather": weather,
            "hot_ratio": round(hot_ratio, 2)
        },
        "data": data_list
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)
        
    print(f"✅ 任务完成！数据已保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
