import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import japanize_matplotlib  # 文字化け防止

# ==========================================
# 1回の出水波形を見るグラフ（時間変化）
# ==========================================

def plot_hyetograph():
    """1. ハイエトグラフ（時間雨量・降雨柱状図）"""
    dates = pd.date_range(start="2019-10-11 00:00", periods=72, freq='h')
    rain = np.random.exponential(scale=5, size=len(dates))
    rain[rain < 4] = 0
    rain[30:40] += np.random.uniform(10, 30, 10) # ピーク作成

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(dates, rain, width=1/24, color='skyblue', edgecolor='black')
    
    ax.set_title('1. ハイエトグラフ（令和元年10月洪水 想定）', fontsize=14)
    ax.set_ylabel('時間雨量 (mm)', fontsize=12)
    ax.invert_yaxis() # 上から垂らす
    ax.xaxis.tick_top()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='left')
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

def plot_hydrograph():
    """2. ハイドログラフ（流量波形）"""
    dates = pd.date_range(start="2019-10-11 00:00", periods=72, freq='h')
    t = np.linspace(0, 100, len(dates))
    flow = 10000 * np.exp(-((t - 50)**2) / (2 * 10**2)) + 150

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, flow, color='blue', linewidth=2, label='観測流量')
    
    ax.axhline(y=9200, color='red', linestyle='-.', linewidth=1.5)
    ax.text(dates[10], 9350, '計画高水流量 9,200m³/s', color='red')

    ax.set_title('2. ハイドログラフ（流量波形）', fontsize=14)
    ax.set_ylabel('流量 (m³/s)', fontsize=12)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.legend(loc='upper right')
    ax.grid(True, linestyle='-', alpha=0.5)
    plt.tight_layout()
    plt.show()

def plot_water_level_hydro():
    """3. 水位波形（水位ハイドログラフ）"""
    dates = pd.date_range(start="2019-10-11 00:00", periods=72, freq='h')
    t = np.linspace(0, 100, len(dates))
    water_level = 6.0 * np.exp(-((t - 50)**2) / (2 * 12**2)) + 1.0 # 平水時1.0m、ピーク約7.0m

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, water_level, color='darkcyan', linewidth=2, label='観測水位')
    
    # 水位特有の警戒基準線
    ax.axhline(y=6.5, color='red', linestyle='--', linewidth=1.5)
    ax.text(dates[5], 6.6, '計画高水位 (HWL) 6.50m', color='red')
    ax.axhline(y=5.0, color='orange', linestyle='--', linewidth=1.5)
    ax.text(dates[5], 5.1, '避難判断水位 5.00m', color='orange')

    ax.set_title('3. 水位波形（水位ハイドログラフ）', fontsize=14)
    ax.set_ylabel('水位 (T.P. m)', fontsize=12)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.legend(loc='upper right')
    ax.grid(True, linestyle='-', alpha=0.5)
    plt.tight_layout()
    plt.show()

# ==========================================
# 数十年間の歴史を見るグラフ（経年変化図）
# ==========================================

def get_dummy_years():
    """経年変化図用のX軸ラベル（S50〜R05）を生成"""
    return [f"S{i}" for i in range(50, 64)] + [f"H{i:02d}" for i in range(1, 31)] + [f"R{i:02d}" for i in range(1, 6)]

def clean_x_labels(ax):
    """X軸のラベルを1つ飛ばしにして見やすくする"""
    ax.tick_params(axis='x', rotation=90)
    labels = [item.get_text() for item in ax.get_xticklabels()]
    for i in range(len(labels)):
        if i % 2 != 0: labels[i] = ""
    ax.set_xticklabels(labels)
    ax.set_xlim(-1, len(labels))

def plot_annual_max_rain():
    """4. 年最大雨量の経年変化図"""
    years = get_dummy_years()
    rain = np.random.normal(loc=150, scale=40, size=len(years))
    rain[years.index('R01')] = 320 # 極端な大雨の年

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(years, rain, color='cornflowerblue', width=0.8)
    
    ax.axhline(y=250, color='red', linestyle='-', linewidth=1.5)
    ax.text(0.5, 260, '計画降雨量 250mm/2日', color='red')

    ax.set_title('4. 年最大雨量の経年変化図', fontsize=14)
    ax.set_ylabel('年最大雨量 (mm)', fontsize=12)
    clean_x_labels(ax)
    ax.grid(axis='y', linestyle=':', alpha=0.7)
    plt.tight_layout()
    plt.show()

def plot_annual_max_flow():
    """5. 年最大流量の経年変化図"""
    years = get_dummy_years()
    flow = np.random.normal(loc=3000, scale=1000, size=len(years))
    flow[years.index('R01')] = 9500

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(years, flow, color='blue', width=0.8)
    
    ax.axhline(y=8100, color='green', linestyle='-', linewidth=1.5)
    ax.text(0.5, 8300, '整備計画流量 8,100m³/s', color='green')

    ax.set_title('5. 年最大流量の経年変化図', fontsize=14)
    ax.set_ylabel('年最大流量 (m³/s)', fontsize=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))
    clean_x_labels(ax)
    ax.grid(axis='y', linestyle=':', alpha=0.7)
    plt.tight_layout()
    plt.show()

def plot_annual_max_water_level():
    """6. 年最高水位の経年変化図"""
    years = get_dummy_years()
    wl = np.random.normal(loc=3.5, scale=1.0, size=len(years))
    wl[years.index('R01')] = 7.2 # HWL突破

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(years, wl, color='darkcyan', width=0.8)
    
    ax.axhline(y=6.5, color='red', linestyle='-', linewidth=1.5)
    ax.text(0.5, 6.6, '計画高水位 (HWL) 6.50m', color='red')

    ax.set_title('6. 年最高水位の経年変化図', fontsize=14)
    ax.set_ylabel('年最高水位 (T.P. m)', fontsize=12)
    clean_x_labels(ax)
    ax.grid(axis='y', linestyle=':', alpha=0.7)
    plt.tight_layout()
    plt.show()


# ==========================================
# 実行部（CLI対話型インターフェース）
# ==========================================
if __name__ == "__main__":
    print("========================================")
    print(" 河川・水文データ グラフ描画ツール")
    print("========================================")
    print("1: ハイエトグラフ（時間雨量）")
    print("2: ハイドログラフ（流量波形）")
    print("3: 水位波形（水位ハイドログラフ）")
    print("4: 年最大雨量の経年変化図")
    print("5: 年最大流量の経年変化図")
    print("6: 年最高水位の経年変化図")
    print("0: 終了")
    print("========================================")
    
    while True:
        # CLIでユーザーからの入力を受け付ける
        target_pattern = input("\n描画したいグラフの番号（1〜6）を入力してください（0で終了）: ")
        
        if target_pattern == '0':
            print("ツールを終了します。")
            break
        elif target_pattern == '1':
            print("ハイエトグラフを描画します...")
            plot_hyetograph()
        elif target_pattern == '2':
            print("ハイドログラフを描画します...")
            plot_hydrograph()
        elif target_pattern == '3':
            print("水位波形を描画します...")
            plot_water_level_hydro()
        elif target_pattern == '4':
            print("年最大雨量の経年変化図を描画します...")
            plot_annual_max_rain()
        elif target_pattern == '5':
            print("年最大流量の経年変化図を描画します...")
            plot_annual_max_flow()
        elif target_pattern == '6':
            print("年最高水位の経年変化図を描画します...")
            plot_annual_max_water_level()
        else:
            print("無効な入力です。0〜6の半角数字を入力してください。")