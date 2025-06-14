import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

palette = ["#d55e00", "#949494"]
rc = {'figure.figsize': (8.9, 4.125), 'xtick.bottom': True, 'ytick.left': True}
sns.set_theme(context="paper", style="whitegrid", palette=palette, font="Times New Roman", font_scale=1.5, rc=rc)

d_rws1 = {
    'step': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003135', '20240319T003140', '20240319T003146', '20240319T003151', '20240319T003156',
        '20240319T003201', '20240319T003205', '20240319T003211', '20240319T003215', '20240319T003220'
    ],
    'average_iops': [3375, 5275, 5074, 4738, 5471, 5861, 5458, 5087, 6383, 4933],
    'total_errors': [2049, 2699, 2331, 2789, 2142, 2414, 2468, 2433, 2589, 2397]
}
df_rws1 = pd.DataFrame(data=d_rws1)

d_rws2 = {
    'step': [2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003237', '20240319T003244', '20240319T003248', '20240319T003253', '20240319T003258',
        '20240319T003304', '20240319T003309', '20240319T003314', '20240319T003319', '20240319T003324'
    ],
    'average_iops': [6415, 4328, 6767, 5573, 5572, 5615, 5500, 5852, 5400, 7170],
    'total_errors': [2583, 2650, 2482, 2923, 2641, 2484, 2789, 2860, 2713, 2664]
}
df_rws2 = pd.DataFrame(data=d_rws2)

d_rws3 = {
    'step': [3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003343', '20240319T003347', '20240319T003352', '20240319T003356', '20240319T003402',
        '20240319T003406', '20240319T003411', '20240319T003416', '20240319T003421', '20240319T003426'
    ],
    'average_iops': [6779, 6812, 6789, 8928, 5586, 7518, 6216, 6292, 6777, 5983],
    'total_errors': [2517, 2752, 2767, 2543, 2763, 4051, 2976, 2878, 2853, 2713]
}
df_rws3 = pd.DataFrame(data=d_rws3)

d_rws4 = {
    'step': [4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003444', '20240319T003449', '20240319T003454', '20240319T003459', '20240319T003504',
        '20240319T003510', '20240319T003515', '20240319T003519', '20240319T003525', '20240319T003530'
    ],
    'average_iops': [5955, 7580, 8004, 6459, 6646, 6186, 6329, 8433, 6526, 6026],
    'total_errors': [2878, 3412, 3016, 3122, 2807, 3311, 3129, 4940, 3304, 4632]
}
df_rws4 = pd.DataFrame(data=d_rws4)

d_rws5 = {
    'step': [5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003549', '20240319T003554', '20240319T003559', '20240319T003604', '20240319T003609',
        '20240319T003615', '20240319T003620', '20240319T003625', '20240319T003630', '20240319T003636'
    ],
    'average_iops': [7559, 7739, 6528, 7914, 6958, 7014, 7417, 7401, 6504, 6698],
    'total_errors': [3288, 3253, 3595, 3124, 2998, 2959, 3403, 3803, 3376, 3073]
}
df_rws5 = pd.DataFrame(data=d_rws5)

d_rws6 = {
    'step': [6, 6, 6, 6, 6, 6, 6, 6, 6, 6],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003654', '20240319T003658', '20240319T003703', '20240319T003708', '20240319T003713',
        '20240319T003718', '20240319T003722', '20240319T003726', '20240319T003730', '20240319T003734'
    ],
    'average_iops': [7959, 10834, 8620, 8431, 7631, 8212, 10169, 9143, 10190, 10676],
    'total_errors': [4165, 3802, 3418, 3238, 4204, 3423, 3656, 3462, 3502, 3397]
}
df_rws6 = pd.DataFrame(data=d_rws6)

d_rws7 = {
    'step': [7, 7, 7, 7, 7, 7, 7, 7, 7, 7],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003751', '20240319T003756', '20240319T003759', '20240319T003803', '20240319T003807',
        '20240319T003811', '20240319T003816', '20240319T003821', '20240319T003826', '20240319T003830'
    ],
    'average_iops': [9595, 10723, 12588, 15016, 10981, 10426, 9256, 7984, 8817, 8565],
    'total_errors': [4164, 3521, 3628, 3214, 3753, 3765, 3527, 3488, 3963, 3907]
}
df_rws7 = pd.DataFrame(data=d_rws7)

d_rws8 = {
    'step': [8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003848', '20240319T003852', '20240319T003857', '20240319T003901', '20240319T003906',
        '20240319T003911', '20240319T003916', '20240319T003919', '20240319T003923', '20240319T003928'
    ],
    'average_iops': [10712, 10999, 11288, 10104, 9376, 8747, 9976, 13643, 11276, 8959],
    'total_errors': [3761, 4402, 3793, 3747, 3794, 3757, 3557, 3779, 4097, 4285]
}
df_rws8 = pd.DataFrame(data=d_rws8)

d_rws9 = {
    'step': [9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T003945', '20240319T003949', '20240319T003953', '20240319T003958', '20240319T004003',
        '20240319T004008', '20240319T004013', '20240319T004017', '20240319T004021', '20240319T004025'
    ],
    'average_iops': [14481, 11494, 11818, 10810, 8183, 9546, 11288, 12012, 13862, 12793],
    'total_errors': [4308, 4003, 4073, 4345, 4614, 4143, 4387, 3757, 4238, 4619]
}
df_rws9 = pd.DataFrame(data=d_rws9)

d_rws10 = {
    'step': [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004042', '20240319T004046', '20240319T004053', '20240319T004057', '20240319T004101',
        '20240319T004106', '20240319T004111', '20240319T004116', '20240319T004120', '20240319T004124'
    ],
    'average_iops': [11793, 11812, 7209, 11660, 12218, 9781, 11326, 10459, 13375, 11222],
    'total_errors': [3811, 3960, 4468, 4200, 4546, 4489, 4972, 5017, 4219, 5158]
}
df_rws10 = pd.DataFrame(data=d_rws10)

d_rws11 = {
    'step': [11, 11, 11, 11, 11, 11, 11, 11, 11, 11],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004141', '20240319T004145', '20240319T004150', '20240319T004154', '20240319T004158',
        '20240319T004202', '20240319T004207', '20240319T004211', '20240319T004216', '20240319T004221'
    ],
    'average_iops': [13540, 13990, 12874, 13976, 12836, 12176, 12224, 14275, 9748, 10908],
    'total_errors': [4120, 4715, 4684, 4276, 5106, 4626, 5273, 5382, 4613, 4572]
}
df_rws11 = pd.DataFrame(data=d_rws11)

d_rws12 = {
    'step': [12, 12, 12, 12, 12, 12, 12, 12, 12, 12],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004237', '20240319T004242', '20240319T004247', '20240319T004252', '20240319T004256',
        '20240319T004300', '20240319T004304', '20240319T004310', '20240319T004314', '20240319T004318'
    ],
    'average_iops': [19453, 12995, 11061, 11000, 14213, 14930, 13902, 10819, 15278, 14726],
    'total_errors': [4320, 5037, 5202, 5103, 4651, 4324, 4789, 4653, 6011, 4330]
}
df_rws12 = pd.DataFrame(data=d_rws12)

d_rws13 = {
    'step': [13, 13, 13, 13, 13, 13, 13, 13, 13, 13],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004336', '20240319T004340', '20240319T004344', '20240319T004348', '20240319T004353',
        '20240319T004357', '20240319T004402', '20240319T004406', '20240319T004410', '20240319T004415'
    ],
    'average_iops': [13414, 14750, 14262, 15482, 13426, 14244, 11720, 14799, 15520, 12873],
    'total_errors': [5047, 4846, 6135, 6830, 5041, 5346, 4937, 4767, 5112, 5338]
}
df_rws13 = pd.DataFrame(data=d_rws13)

d_rws14 = {
    'step': [14, 14, 14, 14, 14, 14, 14, 14, 14, 14],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004433', '20240319T004437', '20240319T004441', '20240319T004447', '20240319T004452',
        '20240319T004456', '20240319T004500', '20240319T004506', '20240319T004511', '20240319T004516'
    ],
    'average_iops': [11485, 14838, 17483, 10019, 12554, 15236, 13731, 10954, 12455, 11330],
    'total_errors': [4793, 5102, 5178, 5198, 4901, 5036, 5484, 5116, 5141, 5375]
}
df_rws14 = pd.DataFrame(data=d_rws14)

d_rws15 = {
    'step': [15, 15, 15, 15, 15, 15, 15, 15, 15, 15],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004533', '20240319T004538', '20240319T004542', '20240319T004547', '20240319T004553',
        '20240319T004557', '20240319T004602', '20240319T004607', '20240319T004611', '20240319T004615'
    ],
    'average_iops': [18313, 12651, 17259, 12213, 12084, 16643, 11425, 13773, 16320, 16472],
    'total_errors': [5038, 5124, 5351, 5576, 5452, 5395, 5599, 6258, 5291, 5489]
}
df_rws15 = pd.DataFrame(data=d_rws15)

d_rws16 = {
    'step': [16, 16, 16, 16, 16, 16, 16, 16, 16, 16],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004632', '20240319T004637', '20240319T004641', '20240319T004646', '20240319T004650',
        '20240319T004654', '20240319T004659', '20240319T004703', '20240319T004708', '20240319T004712'
    ],
    'average_iops': [16265, 15964, 14880, 13322, 16672, 17064, 14421, 15323, 14430, 16539],
    'total_errors': [5979, 5449, 5283, 5346, 6171, 6016, 5324, 5917, 5967, 7040]
}
df_rws16 = pd.DataFrame(data=d_rws16)

d_rws17 = {
    'step': [17, 17, 17, 17, 17, 17, 17, 17, 17, 17],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004730', '20240319T004735', '20240319T004742', '20240319T004747', '20240319T004752',
        '20240319T004757', '20240319T004801', '20240319T004806', '20240319T004810', '20240319T004815'
    ],
    'average_iops': [15384, 16655, 8393, 13164, 15824, 13488, 15421, 16741, 15709, 14598],
    'total_errors': [5476, 6250, 6259, 6520, 5455, 6410, 6516, 6085, 5987, 5932]
}
df_rws17 = pd.DataFrame(data=d_rws17)

d_rws18 = {
    'step': [18, 18, 18, 18, 18, 18, 18, 18, 18, 18],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004841', '20240319T004846', '20240319T004850', '20240319T004854', '20240319T004859',
        '20240319T004904', '20240319T004909', '20240319T004913', '20240319T004917', '20240319T004921'
    ],
    'average_iops': [4492, 13810, 19292, 18665, 16615, 12587, 15634, 20156, 18157, 17601],
    'total_errors': [5419, 5708, 6693, 6000, 6155, 6457, 5789, 6668, 6395, 6002]
}
df_rws18 = pd.DataFrame(data=d_rws18)

d_rws19 = {
    'step': [19, 16, 19, 19, 19, 19, 19, 19, 19, 19],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T004938', '20240319T004943', '20240319T004948', '20240319T004952', '20240319T004957',
        '20240319T005001', '20240319T005008', '20240319T005013', '20240319T005017', '20240319T005022'
    ],
    'average_iops': [16964, 15533, 14748, 19621, 16696, 18829, 8566, 17461, 15936, 16184],
    'total_errors': [6337, 6810, 6167, 6990, 6527, 6063, 6525, 6300, 6679, 6735]
}
df_rws19 = pd.DataFrame(data=d_rws19)

d_rws20 = {
    'step': [20, 20, 20, 20, 20, 20, 20, 20, 20, 20],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005042', '20240319T005046', '20240319T005051', '20240319T005055', '20240319T005100',
        '20240319T005105', '20240319T005110', '20240319T005114', '20240319T005118', '20240319T005122'
    ],
    'average_iops': [12259, 17512, 17308, 17154, 17879, 16022, 15068, 16628, 22011, 19972],
    'total_errors': [6503, 6823, 6742, 7061, 6726, 6892, 6660, 6676, 7473, 6389]
}
df_rws20 = pd.DataFrame(data=d_rws20)

d_rws21 = {
    'step': [21, 21, 21, 21, 21, 21, 21, 21, 21, 21],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005141', '20240319T005145', '20240319T005149', '20240319T005154', '20240319T005159',
        '20240319T005203', '20240319T005207', '20240319T005211', '20240319T005216', '20240319T005220'
    ],
    'average_iops': [18761, 21149, 16792, 16835, 17969, 18761, 22539, 21231, 16606, 18315],
    'total_errors': [6770, 6614, 7847, 6507, 7505, 6648, 6910, 7209, 7413, 7737]
}
df_rws21 = pd.DataFrame(data=d_rws21)

d_rws22 = {
    'step': [22, 22, 22, 22, 22, 22, 22, 22, 22, 22],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005241', '20240319T005245', '20240319T005250', '20240319T005254', '20240319T005259',
        '20240319T005304', '20240319T005309', '20240319T005313', '20240319T005317', '20240319T005322'
    ],
    'average_iops': [17924, 22447, 16698, 19846, 18081, 15274, 16756, 21276, 20489, 20361],
    'total_errors': [6693, 7172, 7599, 7452, 6968, 7216, 7495, 7991, 7608, 7074]
}
df_rws22 = pd.DataFrame(data=d_rws22)

d_rws23 = {
    'step': [23, 23, 23, 23, 23, 23, 23, 23, 23, 23],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005341', '20240319T005345', '20240319T005349', '20240319T005353', '20240319T005357',
        '20240319T005401', '20240319T005406', '20240319T005410', '20240319T005415', '20240319T005445'
    ],
    'average_iops': [15212, 21643, 21709, 22743, 23642, 18996, 17358, 20956, 18327, 17587],
    'total_errors': [6992, 6728, 7442, 7962, 7071, 7569, 8063, 6988, 7215, 7064]
}
df_rws23 = pd.DataFrame(data=d_rws23)

d_rws24 = {
    'step': [24, 24, 24, 24, 24, 24, 24, 24, 24, 24],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005503', '20240319T005508', '20240319T005512', '20240319T005516', '20240319T005521',
        '20240319T005525', '20240319T005531', '20240319T005535', '20240319T005539', '20240319T005543'
    ],
    'average_iops': [18544, 18649, 21876, 21310, 18665, 18965, 16738, 21575, 23939, 19202],
    'total_errors': [7175, 7029, 6961, 7850, 7554, 7927, 7227, 7465, 8073, 7686]
}
df_rws24 = pd.DataFrame(data=d_rws24)

d_rws25 = {
    'step': [25, 25, 25, 25, 25, 25, 25, 25, 25, 25],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005602', '20240319T005607', '20240319T005611', '20240319T005616', '20240319T005621',
        '20240319T005626', '20240319T005631', '20240319T005636', '20240319T005641', '20240319T005645'
    ],
    'average_iops': [18022, 17946, 21137, 17726, 17852, 18348, 22471, 17736, 17548, 25487],
    'total_errors': [7686, 7856, 8219, 7963, 7832, 7633, 8670, 8234, 8039, 7955]
}
df_rws25 = pd.DataFrame(data=d_rws25)

d_rws26 = {
    'step': [26, 26, 26, 26, 26, 26, 26, 26, 26, 26],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005703', '20240319T005708', '20240319T005713', '20240319T005718', '20240319T005723',
        '20240319T005728', '20240319T005732', '20240319T005737', '20240319T005740', '20240319T005745'
    ],
    'average_iops': [18842, 20497, 18661, 18392, 19236, 17326, 24297, 20437, 27184, 19734],
    'total_errors': [7172, 7853, 8027, 8259, 7682, 8051, 7753, 7337, 7982, 8280]
}
df_rws26 = pd.DataFrame(data=d_rws26)

d_rws27 = {
    'step': [27, 27, 27, 27, 27, 27, 27, 27, 27, 27],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005804', '20240319T005809', '20240319T005813', '20240319T005818', '20240319T005822',
        '20240319T005826', '20240319T005831', '20240319T005836', '20240319T005841', '20240319T005846'
    ],
    'average_iops': [20869, 19454, 24716, 19302, 25423, 26124, 19118, 19469, 19082, 18452],
    'total_errors': [7163, 8556, 7836, 9443, 8540, 8607, 8507, 8084, 8477, 8390]
}
df_rws27 = pd.DataFrame(data=d_rws27)

d_rws28 = {
    'step': [28, 28, 28, 28, 28, 28, 28, 28, 28, 28],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T005905', '20240319T005910', '20240319T005915', '20240319T005921', '20240319T005926',
        '20240319T005931', '20240319T005937', '20240319T005943', '20240319T010012', '20240319T010041'
    ],
    'average_iops': [18070, 18527, 18578, 17169, 20240, 17784, 17034, 16433, 19126, 18303],
    'total_errors': [8992, 10323, 9738, 9523, 10045, 10408, 9309, 9820, 8923, 9375]
}
df_rws28 = pd.DataFrame(data=d_rws28)

d_rws29 = {
    'step': [29, 29, 29, 29, 29, 29, 29, 29, 29, 29],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T010100', '20240319T010106', '20240319T010111', '20240319T010140', '20240319T010149',
        '20240319T010154', '20240319T010159', '20240319T010205', '20240319T010210', '20240319T010216'
    ],
    'average_iops': [20736, 18976, 18853, 15647, 10764, 18370, 18719, 19014, 18300, 18632],
    'total_errors': [8448, 9406, 10266, 11925, 9637, 9426, 10550, 10413, 10135, 9766]
}
df_rws29 = pd.DataFrame(data=d_rws29)

d_rws30 = {
    'step': [30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T010259', '20240319T010305', '20240319T010335', '20240319T010341', '20240319T010410',
        '20240319T010518', '20240319T010548', '20240319T010617', '20240319T010623', '20240319T010628'
    ],
    'average_iops': [18705, 19075, 18606, 16652, 18335, 11710, 18038, 17707, 19553, 18301],
    'total_errors': [9454, 10221, 10509, 10868, 10099, 10073, 9740, 9697, 9542, 10249]
}
df_rws30 = pd.DataFrame(data=d_rws30)

d_rws31 = {
    'step': [31, 31, 31, 31, 31, 31, 31, 31, 31, 31],
    'repetition': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'timestamp': [
        '20240319T010647', '20240319T010653', '20240319T010659', '20240319T010704', '20240319T010709',
        '20240319T010714', '20240319T010720', '20240319T010725', '20240319T010730', '20240319T010735'
    ],
    'average_iops': [21231, 19333, 15579, 22624, 20423, 20045, 19995, 19024, 21390, 19960],
    'total_errors': [8789, 9144, 9322, 9478, 9223, 9184, 9707, 9698, 8715, 9702]
}
df_rws31 = pd.DataFrame(data=d_rws31)

dfs_rws = [
    df_rws1, df_rws2, df_rws3, df_rws4, df_rws5, df_rws6, df_rws7, df_rws8, df_rws9, df_rws10, df_rws11, df_rws12,
    df_rws13, df_rws14, df_rws15, df_rws16, df_rws17, df_rws18, df_rws19, df_rws20, df_rws21, df_rws22, df_rws23,
    df_rws24, df_rws25, df_rws26, df_rws27, df_rws28, df_rws29, df_rws30, df_rws31
]

df_rws_all = pd.concat(dfs_rws)
df_rws_all["minutes"] = (pd.to_datetime(df_rws_all["timestamp"]) -
                         datetime.datetime(2024, 3, 19, 0, 31, 35)) / np.timedelta64(1, 'm')
df_rws_all["error_rate"] = df_rws_all["total_errors"] / ((18000 +
                                                          (df_rws_all["step"] * 2000)) / df_rws_all["average_iops"])
df_rws_all["request_count"] = (18000 + (df_rws_all["step"] * 2000)) * df_rws_all["repetition"]
df_rws_all["total_request_count"] = df_rws_all["request_count"].cumsum()
df_rws_all["dollar"] = df_rws_all["total_request_count"] * 0.0000004
df_rws_all["sliding10_average_iops"] = df_rws_all.rolling(window=10)['average_iops'].mean()
print(df_rws_all.to_string())

df_rws_success = df_rws_all[["minutes", "average_iops"]].copy()
df_rws_success.rename(columns={"average_iops": "requests"}, inplace=True)
df_rws_success["outcome"] = "Success"

df_rws_error = df_rws_all[["minutes", "error_rate"]].copy()
df_rws_error.rename(columns={"error_rate": "requests"}, inplace=True)
df_rws_error["outcome"] = "Error"

dfs_rws_request = [df_rws_success, df_rws_error]
df_rws_request_all = pd.concat(dfs_rws_request)
# print(df_rws_request_all.to_string())

ax = sns.lineplot(data=df_rws_request_all, x="minutes", y="requests", hue="outcome")
scale_y = 1e3
ticks_y = ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x / scale_y))
ax.yaxis.set_major_formatter(ticks_y)
ax.set(xlabel="Time [minutes]", ylabel="Thousand IOPS")
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[2:], labels=labels[2:])
sns.move_legend(ax, "upper center", bbox_to_anchor=(0.355, 1))
ax.hlines(y=[5500, 11000, 16500, 22000, 27500], xmin=0, xmax=30, color="grey", linestyle="dashed", linewidth=1)

plt.xlim(xmin=0.0, xmax=30.0)
plt.ylim(ymin=0)
plt.text(29.75, 5000, '1 prefix', ha='right', va='top')
plt.text(29.75, 10500, '2 prefixes', ha='right', va='top')
plt.text(0.25, 16000, '3 prefixes', ha='left', va='top')
plt.text(0.25, 21500, '4 prefixes', ha='left', va='top')
plt.text(0.25, 27000, '5 prefixes', ha='left', va='top')
plt.text(17.5, 7500, 'stragglers', ha='left', va='top')
plt.plot([16.1165], [8393], 'o', ms=15, mec='black', mfc='none')
plt.plot([16.475, 17.45], [7950, 6900], 'black', linewidth=1)
plt.plot([17.1], [4492], 'o', ms=15, mec='black', mfc='none')
plt.plot([17.25, 17.60], [5150, 6250], 'black', linewidth=1)
plt.plot([18.55], [8566], 'o', ms=15, mec='black', mfc='none')
plt.plot([18.55, 18.55], [7900, 7350], 'black', linewidth=1)
plt.rcParams['ytick.major.size'] = 8

# plt.show()
plt.savefig("4_4_object_storage_warming.pdf", bbox_inches="tight")
