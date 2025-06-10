import json
from matplotlib.patches import PathPatch
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

palette = ['#0173b2', '#de8f05']
rc = {'figure.figsize': (8.9, 4.125), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)


def json_to_dataframe(files, reads):
    data_list = []

    for json_file in files:
        with open(json_file, 'r') as f:
            data = json.load(f)
        runs = data['runs']
        run = runs[reads]
        repetitions = run['repetitions']
        for repetition in repetitions:
            invocations = repetition['invocations']
            for invocation in invocations:
                latencies = invocation['invocation_request_latencies_ms']
                for latency in latencies:
                    data_list.append(latency)

    df = pd.DataFrame(columns=["latency"], data=data_list)
    return df


json_s3s_latency = [
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1a.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1b.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1c.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1d.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1e.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1f.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1g.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1h.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1i.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3s_latency_1j.json'
]

json_s3e_latency = [
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1a.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1b.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1c.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1d.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1e.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1f.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1g.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1h.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1i.json',
    '../../../resources/benchmark/measurement/storage_latency/s3/s3e_latency_1j.json'
]

json_ddb_latency = [
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1a.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1b.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1c.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1d.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1e.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1f.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1g.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1h.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1i.json',
    '../../../resources/benchmark/measurement/storage_latency/dynamodb/ddb_latency_1j.json'
]

json_efs_latency = [
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1a.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1b.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1c.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1d.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1e.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1f.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1g.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1h.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1i.json',
    '../../../resources/benchmark/measurement/storage_latency/efs/efs_latency_1j.json'
]

df_s3s_read_latency = json_to_dataframe(json_s3s_latency, 1)
df_s3s_read_latency["storage_type"] = "S3 Standard"
df_s3s_read_latency["operation"] = "Read"

df_s3s_write_latency = json_to_dataframe(json_s3s_latency, 0)
df_s3s_write_latency["storage_type"] = "S3 Standard"
df_s3s_write_latency["operation"] = "Write"

df_s3e_read_latency = json_to_dataframe(json_s3e_latency, 1)
df_s3e_read_latency["storage_type"] = "S3 Express"
df_s3e_read_latency["operation"] = "Read"

df_s3e_write_latency = json_to_dataframe(json_s3e_latency, 0)
df_s3e_write_latency["storage_type"] = "S3 Express"
df_s3e_write_latency["operation"] = "Write"

df_ddb_read_latency = json_to_dataframe(json_ddb_latency, 1)
df_ddb_read_latency["storage_type"] = "DynamoDB"
df_ddb_read_latency["operation"] = "Read"

df_ddb_write_latency = json_to_dataframe(json_ddb_latency, 0)
df_ddb_write_latency["storage_type"] = "DynamoDB"
df_ddb_write_latency["operation"] = "Write"

df_efs_read_latency = json_to_dataframe(json_efs_latency, 1)
df_efs_read_latency["storage_type"] = "EFS"
df_efs_read_latency["operation"] = "Read"

df_efs_write_latency = json_to_dataframe(json_efs_latency, 0)
df_efs_write_latency["storage_type"] = "EFS"
df_efs_write_latency["operation"] = "Write"

dfs_latency = [
    df_s3s_read_latency.sample(frac=0.1),
    df_s3s_write_latency.sample(frac=0.1),
    df_s3e_read_latency.sample(frac=0.1),
    df_s3e_write_latency.sample(frac=0.1),
    df_ddb_read_latency.sample(frac=0.1),
    df_ddb_write_latency.sample(frac=0.1),
    df_efs_read_latency.sample(frac=0.1),
    df_efs_write_latency.sample(frac=0.1)
]
dfs_latency_all = pd.concat(dfs_latency)

# print(dfs_latency_all)
# print(df_s3s_read_latency.latency.quantile(0.9999))
# print(df_s3e_read_latency.latency.quantile(0.9999))
# print(df_ddb_read_latency.latency.quantile(0.9999))
# print(df_efs_read_latency.latency.quantile(0.9999))

flierprops = dict(marker='o', markersize=0.25)
medianprops = dict(linewidth=1)
ax = sns.boxplot(data=dfs_latency_all,
                 x="storage_type",
                 y="latency",
                 hue="operation",
                 width=0.75,
                 fill=False,
                 gap=.1,
                 flierprops=flierprops,
                 medianprops=medianprops)
ax.set_yscale('log')
ax.set_yticks([1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000])
ax.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
ax.set_yticklabels(["1", "2.5", "5", "10", "25", "50", "100", "250", "500", ">=1000"])
ax.legend().set_title('')
plt.legend(loc='upper center', ncol=2)

plt.ylim(ymin=0, ymax=1000)
plt.xlabel('Storage System')
plt.ylabel('Latency [milliseconds] [log scale]')


def adjust_box_widths(ax, fac):
    for c in ax.get_children():
        if isinstance(c, PathPatch):
            p = c.get_path()
            verts = p.vertices
            verts_sub = verts[:-1]
            xmin = np.min(verts_sub[:, 0])
            xmax = np.max(verts_sub[:, 0])
            xmid = 0.5 * (xmin + xmax)
            xhalf = 0.5 * (xmax - xmin)
            xmin_new = xmid - fac * xhalf
            xmax_new = xmid + fac * xhalf
            verts_sub[verts_sub[:, 0] == xmin, 0] = xmin_new
            verts_sub[verts_sub[:, 0] == xmax, 0] = xmax_new
            for l in ax.lines:
                if np.all(l.get_xdata() == [xmin, xmax]):
                    l.set_xdata([xmin_new, xmax_new])


# plt.show()
# plt.savefig("4_3_storage_latency.pdf", bbox_inches="tight")
# plt.savefig("4_3_storage_latency_sampled.pdf", bbox_inches="tight")
plt.savefig("4_3_storage_latency.png", dpi=1200, bbox_inches="tight")
