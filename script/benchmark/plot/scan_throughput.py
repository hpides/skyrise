import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns

palette = ['#d55e00', '#de8f05', '#0173b2', '#c7ebff']
rc = {'figure.figsize': (8.9, 4.125), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)

d_iops = {
    'storage_type': ['4 (~172 MiB)', '8 (~343 MiB)', '12 (~515 MiB) ', '16 (~686 MiB)'],
    'write_iops': [165784, 109082, 82559, 66752],
    'write_quota': [65969, 47663, 45134, 43188],
    'read_iops': [1228800, 217798, 133238, 111578],
    'read_quota': [385587, 162179, 112984, 90000]
}
df_iops = pd.DataFrame(data=d_iops)

df_read_iops = df_iops[["storage_type", "read_iops"]].copy()
df_read_iops.rename(columns={"read_iops": "iops"}, inplace=True)
df_read_iops["Operation"] = "Network"

df_read_quota = df_iops[["storage_type", "read_quota"]].copy()
df_read_quota.rename(columns={"read_quota": "iops"}, inplace=True)
df_read_quota["Operation"] = "I/O Stack"

df_write_iops = df_iops[["storage_type", "write_iops"]].copy()
df_write_iops.rename(columns={"write_iops": "iops"}, inplace=True)
df_write_iops["Operation"] = "Scan"

df_write_quota = df_iops[["storage_type", "write_quota"]].copy()
df_write_quota.rename(columns={"write_quota": "iops"}, inplace=True)
df_write_quota["Operation"] = "Query"

dfs_iops = [df_read_iops, df_read_quota, df_write_iops, df_write_quota]
dfs_iops_all = pd.concat(dfs_iops)
dfs_iops_all['kiops'] = dfs_iops_all['iops'] / 1000
print(dfs_iops_all)

ax = sns.barplot(data=dfs_iops_all, x="storage_type", y="kiops", hue="Operation", width=.65)
ax.set_yscale('log')
ax.set_yticks([25, 50, 75, 150, 300, 600, 1200])
ax.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
ax.legend().set_title('')
plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.15))

ax.bar_label(ax.containers[0],
             color='white',
             padding=91,
             label_type='center',
             rotation=90,
             fmt=lambda x: f'{x:.0f}'[-4:] if x > 1000 else f'')

plt.ylim(ymin=0, ymax=750)
plt.xlabel('# of Partitions per Worker')
plt.ylabel('Worker Throughput [MiB/s] [log scale]')

# plt.show()
plt.savefig("4_5_scan_throughput.pdf", bbox_inches="tight")
