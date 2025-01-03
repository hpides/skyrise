import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

sns.set_context("paper")
sns.set_theme(rc={'figure.figsize': (8.9, 5.5)})
sns.set_palette("colorblind")
sns.set(style="whitegrid", font="Times New Roman", font_scale=1.5)
plt.rcParams['xtick.bottom'] = True
plt.rcParams['ytick.left'] = True
colors = ['#0173B2', '#DE8F05', '#029E73', '#D55E00', '#CC78BC']
legend_write = ['Write', 'Write', 'Write', 'Write', 'Write']
legend_read = ['Read', 'Read', 'Read', 'Read', 'Read']

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

d_efs_sharded = {
    'storage_type': [
        'efs_sharded', 'efs_sharded', 'efs_sharded', 'efs_sharded', 'efs_sharded', 'efs_sharded', 'efs_sharded',
        'efs_sharded'
    ],
    'concurrent_instance_count': [1, 2, 4, 8, 16, 32, 64, 128],
    'write_throughput_mbps': [549, 928, 1770, 2822, 3326, 3719, 3784,
                              float('nan')],
    'read_throughput_mbps': [1037, 1449, 3088, 6070, 10713, 18375, 22317,
                             float('nan')]
}
df_efs_sharded = pd.DataFrame(data=d_efs_sharded)

d_ddb = {
    'storage_type': ['ddb', 'ddb', 'ddb', 'ddb', 'ddb', 'ddb', 'ddb', 'ddb'],
    'concurrent_instance_count': [1, 2, 4, 8, 16, 32, 64, 128],
    'write_throughput_mbps': [29, 27, 28, 30, 26, float('nan'),
                              float('nan'), float('nan')],
    'read_throughput_mbps': [727, 531, 420, 424, 388, float('nan'),
                             float('nan'), float('nan')]
}
df_ddb = pd.DataFrame(data=d_ddb)

dfs_sts = [df_s3s, df_s3e, df_efs, df_efs_sharded, df_ddb]

df_sts_all = pd.concat(dfs_sts)
df_sts_all['write_throughput_gbps'] = df_sts_all['write_throughput_mbps'] / 1024
df_sts_all['read_throughput_gbps'] = df_sts_all['read_throughput_mbps'] / 1024
print(df_sts_all.to_string())

fig, ax = plt.subplots()

ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3s'],
        label=legend_read[0],
        color=colors[0],
        marker='o')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3e'],
        label=legend_read[1],
        color=colors[1],
        marker='s')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'efs'],
        label=legend_read[2],
        color=colors[2],
        marker='d')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'efs_sharded'],
        label=legend_read[3],
        color=colors[3],
        marker='d')
ax.plot('concurrent_instance_count',
        'read_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'ddb'],
        label=legend_read[4],
        color=colors[4],
        marker='*')

ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3s'],
        label=legend_write[0],
        color=colors[0],
        marker='o',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 's3e'],
        label=legend_write[1],
        color=colors[1],
        marker='s',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'efs'],
        label=legend_write[2],
        color=colors[2],
        marker='d',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'efs_sharded'],
        label=legend_write[3],
        color=colors[3],
        marker='d',
        linestyle='dashdot')
ax.plot('concurrent_instance_count',
        'write_throughput_gbps',
        data=df_sts_all[df_sts_all['storage_type'] == 'ddb'],
        label=legend_write[4],
        color=colors[4],
        marker='*',
        linestyle='dashdot')

h, l = ax.get_legend_handles_labels()
ph = [plt.plot([], marker="", linestyle="")[0]] * 5
handles = ph + h
labels = ["S3 Standard:", "S3 Express:", "EFS-1:", "EFS-2:", "DynamoDB:"] + l

ax.set_yscale('log')
ax.set_yticks([0, 1, 5, 25, 125, 250])
ax.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
ax.hlines(y=[5, 20, 224, 352], xmin=0, xmax=128, color="grey", linestyle="dashed", linewidth=1)

plt.text(2.5, 332, 'Per-Partition Read IOPS in S3', ha='left', va='top', fontsize=13)
plt.text(2.5, 209, 'Per-Partition Write IOPS in S3', ha='left', va='top', fontsize=13)
plt.text(127, 18, 'Per-FS Read Throughput in EFS', ha='right', va='top', fontsize=13)
plt.text(127, 4.5, 'Per-FS Write Throughput in EFS', ha='right', va='top', fontsize=13)
plt.text(13.5, 0.125, '12X Read:Write Ratio', ha='left', va='top', fontsize=13)
plt.arrow(12, 0.1, 0, 0.15, width=0.5, head_length=0.1, head_width=2.5, color='#949494')
plt.arrow(12, 0.1, 0, -0.057, width=0.5, head_length=0.012, head_width=2.5, color='#949494')

plt.xticks([1, 4, 8, 16, 32, 64, 128])
plt.xlim(xmin=1, xmax=128)
plt.ylim(ymin=0, ymax=400)
plt.xlabel('# of Instances')
plt.ylabel('Throughput [GiB/s] [log scale]')
leg = plt.legend(handles, labels, ncol=3, columnspacing=0.75, labelspacing=0.45)
for vpack in leg._legend_handle_box.get_children()[:1]:
    for hpack in vpack.get_children():
        hpack.get_children()[0].set_width(0)

#ax = sns.lineplot(data=df_sts_all, x="concurrent_instance_count", y="write_throughput_mbps", hue="storage_type")
#ax.set_yscale('log')
#scale_y = 1e3
#ticks_y = ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x/scale_y))
#ax.yaxis.set_major_formatter(ticks_y)
#ax.set(xlabel="# of Instances", ylabel="Throughput (GiB/s)")

#handles, labels = ax.get_legend_handles_labels()
#ax.legend(handles=handles[4:], labels=labels[4:])

#plt.rcParams['ytick.major.size'] = 8

#plt.show()
plt.savefig("4_3_storage_throughput.pdf", bbox_inches="tight")
