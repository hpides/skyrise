import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns

palette = ['#0173b2', '#c7ebff', '#de8f05', '#feedd0']
rc = {'figure.figsize': (8.9, 4.125), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)

d_iops = {
    'storage_type': ['S3 Standard', 'S3 Express', 'DynamoDB', 'EFS-1', 'EFS-2'],
    'write_iops': [4099, 42309, 9603, 3662, 4464],
    'write_quota': [3500, 0, 4000, 50000, 100000],
    'read_iops': [8307, 220183, 16177, 7028, 13913],
    'read_quota': [5500, 0, 12000, 90000, 180000]
}
df_iops = pd.DataFrame(data=d_iops)

df_read_iops = df_iops[["storage_type", "read_iops"]].copy()
df_read_iops.rename(columns={"read_iops": "iops"}, inplace=True)
df_read_iops["Operation"] = "Read"

df_read_quota = df_iops[["storage_type", "read_quota"]].copy()
df_read_quota.rename(columns={"read_quota": "iops"}, inplace=True)
df_read_quota["Operation"] = "Read Quota"

df_write_iops = df_iops[["storage_type", "write_iops"]].copy()
df_write_iops.rename(columns={"write_iops": "iops"}, inplace=True)
df_write_iops["Operation"] = "Write"

df_write_quota = df_iops[["storage_type", "write_quota"]].copy()
df_write_quota.rename(columns={"write_quota": "iops"}, inplace=True)
df_write_quota["Operation"] = "Write Quota"

dfs_iops = [df_read_iops, df_read_quota, df_write_iops, df_write_quota]
dfs_iops_all = pd.concat(dfs_iops)
dfs_iops_all['kiops'] = dfs_iops_all['iops'] / 1000
print(dfs_iops_all)

ax = sns.barplot(data=dfs_iops_all, x="storage_type", y="kiops", hue="Operation", width=.65)
ax.set_yscale('log')
ax.set_yticks([1, 5, 25, 125, 250])
ax.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
ax.legend().set_title('')
plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.16))

ax.bar_label(ax.containers[0],
             color='grey',
             padding=4,
             label_type='edge',
             rotation=90,
             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])
ax.bar_label(ax.containers[1],
             color='grey',
             padding=4,
             label_type='edge',
             rotation=90,
             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])
ax.bar_label(ax.containers[2],
             color='grey',
             padding=4,
             label_type='edge',
             rotation=90,
             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])
ax.bar_label(ax.containers[3],
             color='grey',
             padding=4,
             label_type='edge',
             rotation=90,
             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])

plt.ylim(ymin=0, ymax=550)
plt.xlabel('Storage System')
plt.ylabel('Thousand IOPS [log scale]')

# plt.show()
plt.savefig("4_3_storage_iops.pdf", bbox_inches="tight")
