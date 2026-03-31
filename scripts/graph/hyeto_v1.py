import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 日本語フォントの設定（環境に合わせて変更してください。japanize_matplotlib推奨）
try:
    import japanize_matplotlib
except ImportError:
    plt.rcParams['font.family'] = 'sans-serif'
    # Windowsの場合は 'MS Gothic' や 'Meiryo' など
    # plt.rcParams['font.family'] = 'Meiryo' 

def create_hyetograph(is_top_down=True):
    # 1. ダミーデータの生成（5日間、1時間間隔）
    dates = pd.date_range(start="2024-07-04 00:00", periods=5*24, freq='h')
    
    # 集中豪雨っぽい適当な雨量データを作成
    np.random.seed(42)
    hourly_rain = np.random.exponential(scale=3, size=len(dates))
    hourly_rain[hourly_rain < 3.5] = 0  # 降っていない時間を多めにする
    
    # ピークを人工的につくる（7/5〜7/6あたり）
    hourly_rain[30:50] += np.random.uniform(5, 25, 20)
    
    # 累加雨量を計算
    cumulative_rain = hourly_rain.cumsum()

    # 2. グラフの描画設定
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax2 = ax1.twinx()  # 累加雨量用に第2Y軸を作成

    # 時間雨量（棒グラフ）
    # widthは1日が'1'となるため、1時間=1/24に設定
    ax1.bar(dates, hourly_rain, width=1/24, color='skyblue', edgecolor='black', label='時間雨量')

    # 累加雨量（折れ線グラフ）
    ax2.plot(dates, cumulative_rain, color='magenta', marker='s', markersize=3, linestyle='-', label='累加雨量')

    # 3. 軸とラベルの設定
    ax1.set_ylabel('時間雨量 (mm)')
    ax2.set_ylabel('累加雨量 (mm)')
    
    # 見栄えのためY軸の最大値を少し広げる
    ax1.set_ylim(0, max(hourly_rain) * 1.2)
    ax2.set_ylim(0, max(cumulative_rain) * 1.1)

    # ★ここがポイント：上から垂らすかどうかの切り替え★
    if is_top_down:
        ax1.invert_yaxis() # 時間雨量のY軸を反転
        ax2.invert_yaxis() # 累加雨量のY軸を反転
        ax1.xaxis.tick_top() # X軸の時刻表記を上に持ってくる

    # X軸の時刻フォーマット設定
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6)) # 6時間ごとにメモリを表示
    
    # X軸ラベルを見やすく回転
    if is_top_down:
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='left')
    else:
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 凡例の表示（2つの軸の凡例をまとめる）
    handler1, label1 = ax1.get_legend_handles_labels()
    handler2, label2 = ax2.get_legend_handles_labels()
    ax1.legend(handler1 + handler2, label1 + label2, loc='upper left' if not is_top_down else 'lower right')

    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

# 上から垂れ下がるグラフ（画像と同じ形式）
create_hyetograph(is_top_down=True)

# 下から伸びる通常のグラフ
# create_hyetograph(is_top_down=False)