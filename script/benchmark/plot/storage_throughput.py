import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns

palette = ['#0173B2', '#DE8F05', '#029E73', '#D55E00', '#CC78BC']
rc = {'figure.figsize': (8.9, 4.4), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)

legend_write = ['Write', 'Write', 'Write', 'Write']
legend_read = ['Read', 'Read', 'Read', 'Read']

d_s3s = {
    'storage_type': ['s3s', 's3s', 's3s', 's3s', 's3s', 's3s', 's3s', 's3s'],
    'concurrent_instance_count': [1, 2, 4, 8, 16, 32, 64, 128],
    'write_throughput_mbps': [1566, 3092, 6038, 11639, 22712, 44717, 89191, 179228],
    'read_throughput_mbps': [2596, 5056, 10023, 19897, 39102, 77634, 155258, 248676]
}
df_s3s = pd.DataFrame(data=d_s3s)

d_s3e = {
    'storage_type': ['s3e', 's3e', 's3e', 's3e', 's3e', 's3e', 's3e', 's3e'],
    'concurrent_instance_count': [1, 2, 4, 8, 16, 32, 64, 128],
    'write_throughput_mbps': [2756, 5931, 10437, 20915, 41981, 82209, 172754, 217642],
    'read_throughput_mbps': [2929, 5867, 11370, 22348, 45775, 91319, 184730, 223708]
}
df_s3e = pd.DataFrame(data=d_s3e)

d_efs = {
    'storage_type': ['efs', 'efs', 'efs', 'efs', 'efs', 'efs', 'efs', 'efs'],
    'concurrent_instance_count': [1, 2, 4, 8, 16, 32, 64, 128],
    'write_throughput_mbps': [549, 825, 1399, 1668, 1693, 1884, 1897,
                              float('nan')],
    'read_throughput_mbps': [1037, 2035, 2961, 4968, 8570, 14098, 22676,
                             float('nan')]
}
df_efs = pd.DataFrame(data=d_efs)

d_ddb = {
    'storage_type': ['ddb', 'ddb', 'ddb', 'ddb', 'ddb', 'ddb', 'ddb', 'ddb'],
    'concurrent_instance_count': [1, 2, 4, 8, 16, 32, 64, 128],
    'write_throughput_mbps': [29, 27, 28, 30, 26, float('nan'),
                              float('nan'), float('nan')],
    'read_throughput_mbps': [727, 531, 420, 424, 388, float('nan'),
                             float('nan'), float('nan')]
}
df_ddb = pd.DataFrame(data=d_ddb)

dfs_sts = [df_s3s, df_s3e, df_efs, df_ddb]

df_sts_all = pd.concat(dfs_sts)
df_sts_all['write_throughput_gbps'] = df_sts_all['write_throughput_mbps'] / 1024
df_sts_all['read_throughput_gbps'] = df_sts_all['read_throughput_mbps'] / 1024
print(df_sts_all.to_string())

fig, ax = plt.subplots()

ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3s'],
        label=legend_read[0],
        color=palette[0],
        marker='o')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3e'],
        label=legend_read[1],
        color=palette[1],
        marker='s')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'efs'],
        label=legend_read[2],
        color=palette[2],
        marker='d')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'ddb'],
        label=legend_read[3],
        color=palette[3],
        marker='*')

ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3s'],
        label=legend_write[0],
        color=palette[0],
        marker='o',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3e'],
        label=legend_write[1],
        color=palette[1],
        marker='s',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'efs'],
        label=legend_write[2],
        color=palette[2],
        marker='d',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'ddb'],
        label=legend_write[3],
        color=palette[3],
        marker='*',
        linestyle='dashdot')

h, l = ax.get_legend_handles_labels()
ph = [plt.plot([], marker="", linestyle="")[0]] * 4
handles = ph + h
labels = ["S3 Standard:", "S3 Express:", "EFS:", "DynamoDB:"] + l

ax.set_yticks([0, 50, 100, 150, 200, 250])
ax.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
ax.hlines(y=[5, 20, 224], xmin=0, xmax=128, color="grey", linestyle="dashed", linewidth=2)

plt.text(2.5, 222, 'Per-Partition Write IOPS in S3', ha='left', va='top', fontsize=14)
plt.text(127, 20, 'Per-FS Read Quota in EFS', ha='right', va='bottom', fontsize=14)
plt.text(127, 5, 'Per-FS Write Quota in EFS', ha='right', va='bottom', fontsize=14)

plt.xticks([1, 4, 8, 16, 32, 64, 128])
plt.xlim(xmin=1, xmax=128)
plt.ylim(ymin=0, ymax=250)
plt.xlabel('# of Instances')
plt.ylabel('Throughput [GiB/s]')
leg = plt.legend(handles, labels, ncol=3, columnspacing=0.75, labelspacing=0.1, bbox_to_anchor=(0.20, 0.88))
for vpack in leg._legend_handle_box.get_children()[:1]:
    for hpack in vpack.get_children():
        hpack.get_children()[0].set_width(-13)

# plt.show()
plt.savefig("4_3_storage_throughput.pdf", bbox_inches="tight")
