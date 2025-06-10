import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

palette = ["#56b4e9", "#949494"]
rc = {'figure.figsize': (8.9, 4.125), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)

d_rcs_all = {
    'timestamp': [
        '20240322T104419', '20240322T104419', '20240322T104419', '20240323T104419', '20240323T104419',
        '20240323T104419', '20240324T122808', '20240324T122808', '20240324T122808', '20240325T094738',
        '20240325T094738', '20240325T094738', '20240326T100011', '20240326T100011', '20240326T100011',
        '20240327T104511', '20240327T104511', '20240327T104511'
    ],
    'average_iops': [
        24297, 20437, 27184, 11388, 29753, 25667, 10774, 10538, 2816, 11897, 11804, 2934, 10230, 2966, 1493, 2312, 1918,
        2102
    ],
    'total_errors': [
        7753, 7337, 7982, 75105, 10169, 11023, 61406, 61770, 109229, 69532, 62453, 88575, 84459, 74896, 123232, 92183,
        94821, 102349
    ],
    'series': [
        'daily', 'daily', 'daily', 'daily', 'daily', 'daily', 'daily', 'daily', 'daily', 'daily', 'daily', 'daily',
        'daily', 'daily', 'daily', 'daily', 'daily', 'daily'
    ]
}
df_rcs_all = pd.DataFrame(data=d_rcs_all)

df_rcs_all["hours"] = (pd.to_datetime(df_rcs_all["timestamp"]) -
                       datetime.datetime(2024, 3, 22, 10, 44, 19)) / np.timedelta64(1, 'h')
df_rcs_all["error_rate"] = df_rcs_all["total_errors"] / (100000 / df_rcs_all["average_iops"])

df_rcs_success = df_rcs_all[["hours", "average_iops"]].copy()
df_rcs_success.rename(columns={"average_iops": "requests"}, inplace=True)
df_rcs_success["outcome"] = "Success"

df_rcs_error = df_rcs_all[["hours", "error_rate"]].copy()
df_rcs_error.rename(columns={"error_rate": "requests"}, inplace=True)
df_rcs_error["outcome"] = "Error"

dfs_rcs_request = [df_rcs_success, df_rcs_error]
df_rcs_request_all = pd.concat(dfs_rcs_request)
# print(df_rcs_request_all.to_string())

ax = sns.lineplot(data=df_rcs_request_all, x="hours", y="requests", hue="outcome")
scale_y = 1e3
ticks_y = ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x / scale_y))
ax.yaxis.set_major_formatter(ticks_y)
ax.set(xlabel="Time [hours]", ylabel="Thousand IOPS")
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[2:], labels=labels[2:])
sns.move_legend(ax, "upper center", bbox_to_anchor=(0.665, 1))
ax.hlines(y=[5500, 11000, 16500, 22000, 27500], xmin=0, xmax=120.0, color="grey", linestyle="dashed", linewidth=1)

plt.xlim(xmin=0.0, xmax=120.0)
plt.ylim(ymin=0)
plt.text(119.75, 5000, '1 prefix', ha='right', va='top')
plt.text(119.75, 10500, '2 prefixes', ha='right', va='top')
plt.text(119.75, 16000, '3 prefixes', ha='right', va='top')
plt.text(119.75, 21500, '4 prefixes', ha='right', va='top')
plt.text(119.75, 27000, '5 prefixes', ha='right', va='top')

# plt.show()
plt.savefig("4_4_object_storage_cooling.pdf", bbox_inches="tight")
