import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import scipy.optimize as so
import seaborn as sns

palette = ["#029e73", "#cc78bc"]
rc = {'figure.figsize': (8.9, 4.125), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)

d_cost = {
    'prefix': [1, 2, 3, 4, 5, 6, 7, 8, 9., 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'time': [
        0, 10.34, 21.50, 33.85, 48.75, None, None, None, None, None, None, None, None, None, None, None, None, None,
        None, None
    ],
    'cost': [
        0, 6.3800, 18.4752, 36.5912, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        None, None
    ]
}
df_cost = pd.DataFrame(data=d_cost)
print(df_cost.to_string())

# Extrapolate the cost and time columns with a 3rd order polynomial
# Create copy of data to remove NaNs for curve fitting
fit_df_cost = df_cost.dropna()


# Function to curve fit to the data
def func(x, a, b, c, d):
    return a * (x**3) + b * (x**2) + c * x + d


# Initial parameter guess, just to kick off the optimization
guess = (0.5, 0.5, 0.5, 0.5)
# Place to store function parameters for each column
col_params = {}
# Curve fit each column
for col in fit_df_cost.columns:
    # Get x & y
    x = fit_df_cost.index.astype(float).values
    y = fit_df_cost[col].values
    # Curve fit column and get curve parameters
    params = so.curve_fit(func, x, y, guess)
    # Store optimized parameters
    col_params[col] = params[0]
# Extrapolate each column
for col in df_cost.columns:
    # Get the index values for NaNs in the column
    x = df_cost[pd.isnull(df_cost[col])].index.astype(float).values
    # Extrapolate those points with the fitted function
    df_cost[col][x] = func(x, *col_params[col])
print('Data was extrapolated with these column functions:')
for col in col_params:
    print('f_{}(x) = {:0.3e} x^3 + {:0.3e} x^2 + {:0.4f} x + {:0.4f}'.format(col, *col_params[col]))
print(df_cost.to_string())

ax1 = sns.lineplot(data=df_cost, x="prefix", y="time", linestyle='-.', label="Time", legend=None, linewidth=2)
ax2 = ax1.twinx()
sns.lineplot(data=df_cost, x="prefix", y="cost", color="#cc78bc", ax=ax2, linestyle='-.', label="Cost", linewidth=2)
ax3 = sns.lineplot(data=fit_df_cost, x="prefix", y="time", color="#029e73", legend=None, linewidth=2)
ax4 = sns.lineplot(data=fit_df_cost, x="prefix", y="cost", color="#cc78bc", legend=None, linewidth=2)

ticks_y = ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x / 60))
ax1.yaxis.set_major_formatter(ticks_y)
ax1.set(xlabel="# of Prefixes", ylabel="Time [hours]")
ax1.set_yticks([0, 180, 360, 540, 720, 900, 1080, 1260, 1440])
ax2.set(ylabel="Cost [$]")
ax2.set_yticks([0, 180, 360, 540, 720, 900, 1080, 1260, 1440])
ax2.set_xticks([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20])
handles, labels = ax1.get_legend_handles_labels()
handles2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(handles2 + handles, labels2 + labels, loc=0)
sns.move_legend(ax2, "upper center", bbox_to_anchor=(0.580, 1))
ax2.vlines(x=[4.54, 9.09, 18.18], ymin=0, ymax=1440, color="grey", linewidth=1, linestyle="dashed")
ax1.set_ylim(0, 1440)
ax2.set_ylim(0, 1440)
plt.axvspan(4, 20, facecolor='#949494', alpha=0.1)
plt.text(2.5, 630, 'measured', ha='center', va='center')
plt.text(12, 810, 'extrapolated', ha='center', va='center', color='#949494')
plt.text(4.29, 1350, '25K IOPS', ha='right', va='center')
plt.text(8.84, 1350, '50K IOPS', ha='right', va='center')
plt.text(17.93, 1350, '100K IOPS', ha='right', va='center')
plt.xlim(xmin=1.0, xmax=20.0)

# plt.show()
plt.savefig("4_4_warming_time_and_cost.pdf", bbox_inches="tight")
