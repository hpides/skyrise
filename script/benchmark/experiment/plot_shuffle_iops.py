import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

sns.set_context("paper")
sns.set_theme(rc={'figure.figsize': (8.9, 4.125)})
sns.set(style="whitegrid", font="Times New Roman", font_scale=1.5)
plt.rcParams['xtick.bottom'] = True
plt.rcParams['ytick.left'] = True
sns.set_palette("colorblind")
#colors = ['#0173b2', '#ece133', '#de8f05', '#d55e00']
#colors = ['#0173b2', '#c7ebff', '#de8f05', '#d55e00']
colors = ['#d55e00', '#0173b2', '#c7ebff']

d_iops = {
    'storage_type': ['Initialized Standard', 'Scaled Standard', 'Express'],
    'write_iops': [5550, 6506, 11050],
    'write_quota': [1550, 1661, 1910],
    'read_iops': [8300, 16000, 220000]
}
df_iops = pd.DataFrame(data=d_iops)

df_read_iops = df_iops[["storage_type", "read_iops"]].copy()
df_read_iops.rename(columns={"read_iops": "iops"}, inplace=True)
df_read_iops["Operation"] = "S3"

df_write_iops = df_iops[["storage_type", "write_iops"]].copy()
df_write_iops.rename(columns={"write_iops": "iops"}, inplace=True)
df_write_iops["Operation"] = "Shuffle"

df_write_quota = df_iops[["storage_type", "write_quota"]].copy()
df_write_quota.rename(columns={"write_quota": "iops"}, inplace=True)
df_write_quota["Operation"] = "Query"

dfs_iops = [df_read_iops, df_write_iops, df_write_quota]
dfs_iops_all = pd.concat(dfs_iops)
dfs_iops_all['kiops'] = dfs_iops_all['iops'] / 1000
print(dfs_iops_all)

ax = sns.barplot(data=dfs_iops_all, x="storage_type", y="kiops", palette=colors, hue="Operation", width=.65)
ax.set_yticks([2.5, 5.0, 7.5, 10.0, 12.5, 15.0, 17.5])
ax.legend().set_title('')
plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.15))

ax.bar_label(ax.containers[0],
             color='white',
             padding=95,
             label_type='center',
             rotation=90,
             fmt=lambda x: f'{x:.0f}'[-4:] if x > 200 else f'')
#ax.bar_label(ax.containers[1],
#             color='grey',
#             padding=4,
#             label_type='edge',
#             rotation=90,
#             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])
#ax.bar_label(ax.containers[2],
#             color='grey',
#             padding=4,
#             label_type='edge',
#             rotation=90,
#             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])
#ax.bar_label(ax.containers[3],
#             color='grey',
#             padding=4,
#             label_type='edge',
#             rotation=90,
#             fmt=lambda x: f'{x:.0f}'[-3:] if x > 10 else f'{x:.1f}'[-3:])

plt.ylim(ymin=0, ymax=19)
plt.xlabel('Storage Class and Mode')
plt.ylabel('Thousand Read IOPS')

plt.show()
#plt.savefig("4_5_shuffle_iops.pdf", bbox_inches="tight")
