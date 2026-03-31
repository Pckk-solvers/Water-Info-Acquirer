import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import japanize_matplotlib  # 日本語文字化け防止

# 1. ダミーデータの生成（令和元年10月洪水のような5日間を想定）
dates = pd.date_range(start="2019-10-11 00:00", end="2019-10-16 00:00", freq='1h')
t = np.linspace(0, 100, len(dates))

# 丸森（上流側：ピーク到達が早く、流量は少なめ）
peak_t_marumori = 40
flow_marumori = 6000 * np.exp(-((t - peak_t_marumori)**2) / (2 * 5**2))
# ピーク後の減水部（逓減曲線）を少し緩やかにする
flow_marumori[t > peak_t_marumori] = 6000 * np.exp(-((t[t > peak_t_marumori] - peak_t_marumori)**2) / (2 * 12**2))
flow_marumori += 100 # 平水時のベース流量

# 岩沼（下流側：ピーク到達が遅く、合流により流量が多い）
peak_t_iwanuma = 45
flow_iwanuma = 10000 * np.exp(-((t - peak_t_iwanuma)**2) / (2 * 6**2))
flow_iwanuma[t > peak_t_iwanuma] = 10000 * np.exp(-((t[t > peak_t_iwanuma] - peak_t_iwanuma)**2) / (2 * 15**2))
flow_iwanuma += 150 # 平水時のベース流量


# 2. グラフの描画
fig, ax = plt.subplots(figsize=(10, 5))

# 流量の折れ線グラフを描画
ax.plot(dates, flow_iwanuma, color='blue', linewidth=2, label='岩沼')
ax.plot(dates, flow_marumori, color='deepskyblue', linewidth=2, label='丸森')

# 3. 計画高水流量の水平線（閾値）とテキストの配置
# テキストのX座標（グラフの中央からやや右寄りに配置）
text_x = dates[int(len(dates) * 0.55)]

# 岩沼の計画高水流量
ax.axhline(y=9200, color='red', linestyle='-.', linewidth=1.5)
ax.text(text_x, 9200 + 150, '岩沼地点 計画高水流量 9,200m³/s', 
        color='red', va='bottom', ha='center', fontsize=11)

# 丸森の計画高水流量
ax.axhline(y=7100, color='orange', linestyle='-.', linewidth=1.5)
ax.text(text_x, 7100 + 150, '丸森地点 計画高水流量 7,100m³/s', 
        color='orange', va='bottom', ha='center', fontsize=11)


# 4. 軸と見た目の設定
ax.set_ylabel('流量 (m³/s)', fontsize=12)
ax.set_title('令和元年10月洪水', fontsize=14)
ax.set_ylim(0, 12000)

# X軸の時刻フォーマット設定（日付と時刻）
ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
# 目盛りを1日ごとに設定（画像のように0:00のみ表示させる工夫）
ax.xaxis.set_major_locator(mdates.DayLocator())

# 凡例の設定（右上に配置、枠線を黒色で四角くする）
ax.legend(loc='upper right', edgecolor='black', fancybox=False, framealpha=1.0)

# グリッド線の設定
ax.grid(True, which='major', color='gray', linestyle='-', alpha=0.5)

plt.tight_layout()
plt.show()