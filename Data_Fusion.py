import os
import pandas as pd

"""
1、如果Power_Nas非空，Power_DB为空，则使用Power_Nas的；
2、如果Power_Nas为空，Power_DB非空，则使用DB的；
3、如果Power_Nas和Power_DB都为空，则使用空值；
4、如果Power_Nas和Power_Nas都非空，则采用Power_Nas的。
5、Radiation同理；
"""

# 输入和输出文件夹路径
input_folder = r"D:\新能源预测小组\Project\concat\data\DB+Nas"
output_folder = r"D:\新能源预测小组\Project\concat\data\Fusion"

# 创建输出文件夹（如果不存在）
os.makedirs(output_folder, exist_ok=True)

# 定义时间序列起始时间和间隔
start_time = pd.Timestamp("2021-01-01 00:00")
time_interval = pd.Timedelta(minutes=5)

# 遍历输入文件夹中的所有 CSV 文件
for file_name in os.listdir(input_folder):
    if file_name.endswith(".csv"):
        file_path = os.path.join(input_folder, file_name)
        
        # 读取 CSV 文件
        data = pd.read_csv(file_path)

        # 确保 'Time' 列为 datetime 格式
        data['Time'] = pd.to_datetime(data['Time'], errors='coerce')
        if data['Time'].isna().all():
            print(f"文件 {file_name} 的 'Time' 列无法解析为有效日期，跳过处理")
            continue

        # 自动生成完整时间序列
        full_time_index = pd.date_range(start=start_time, periods=len(data), freq='5min')
        data = data.set_index('Time').reindex(full_time_index).reset_index()
        data.rename(columns={'index': 'Time'}, inplace=True)

        # 按规则融合 Power 列
        data['Power_fusion'] = data.apply(
            lambda row: row['Power_Nas'] if pd.notna(row['Power_Nas']) else row['Power_DB'], axis=1
        )
        
        # 按规则融合 Radiation 列
        data['Radiation_fusion'] = data.apply(
            lambda row: row['Radiation_Nas'] if pd.notna(row['Radiation_Nas']) else row['Radiation_DB'], axis=1
        )

        # 保存结果到输出文件夹
        output_file_path = os.path.join(output_folder, file_name)
        data.to_csv(output_file_path, index=False, encoding='utf-8-sig')
        print(f"文件 {file_name} 已融合并保存至 {output_file_path}")

print("所有文件处理完成！")
