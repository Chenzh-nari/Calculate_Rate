import os
import pandas as pd

# 设置文件路径
db_path = r"D:\新能源预测小组\Project\concat\data\DB"
nas_path = r"D:\新能源预测小组\Project\concat\data\Nas"
output_path = r"D:\新能源预测小组\Project\concat\data\DB+Nas"

# 创建输出目录（如果不存在）
os.makedirs(output_path, exist_ok=True)

# 获取两个目录中的文件名
db_files = {os.path.splitext(f)[0]: os.path.join(db_path, f) for f in os.listdir(db_path) if f.endswith(".csv")}
nas_files = {os.path.splitext(f)[0]: os.path.join(nas_path, f) for f in os.listdir(nas_path) if f.endswith(".csv")}

# 合并所有场站 ID
all_ids = set(db_files.keys()).union(set(nas_files.keys()))

# 遍历所有场站 ID，进行数据处理和合并
for station_id in all_ids:
    # 读取 DB 文件（如果存在）
    if station_id in db_files:
        db_df = pd.read_csv(db_files[station_id])
        if 'Power_DB' in db_df.columns and 'Radiation_DB' in db_df.columns:
            db_df['Time'] = pd.date_range(start='2021-01-01 00:00', periods=len(db_df), freq='5min')  # 使用 5 分钟间隔
        else:
            print(f"Warning: 文件 {db_files[station_id]} 缺少必要的列，跳过处理")
            db_df = pd.DataFrame(columns=['Time', 'Power_DB', 'Radiation_DB'])
    else:
        db_df = pd.DataFrame(columns=['Time', 'Power_DB', 'Radiation_DB'])
    
    # 读取 NAS 文件（如果存在）
    if station_id in nas_files:
        nas_df = pd.read_csv(nas_files[station_id])
        if 'Power_Nas' in nas_df.columns and 'Radiation_Nas' in nas_df.columns:
            nas_df['Time'] = pd.date_range(start='2021-01-01 00:00', periods=len(nas_df), freq='5min')  # 使用 5 分钟间隔
        else:
            print(f"Warning: 文件 {nas_files[station_id]} 缺少必要的列，跳过处理")
            nas_df = pd.DataFrame(columns=['Time', 'Power_Nas', 'Radiation_Nas'])
    else:
        nas_df = pd.DataFrame(columns=['Time', 'Power_Nas', 'Radiation_Nas'])
    
    # 合并数据（基于时间列）
    merged_df = pd.merge(db_df, nas_df, on='Time', how='outer')
    
    # 确保只保留一个时间列
    if 'Time' in merged_df.columns:
        # 如果多余的时间列存在，直接删除
        merged_df.drop(columns=[col for col in merged_df.columns if col.startswith('Time_')], inplace=True)
    
    # 确保时间列在最左边
    cols = ['Time'] + [col for col in merged_df.columns if col != 'Time']
    merged_df = merged_df[cols]
    
    # 将合并后的数据保存到输出目录
    output_file = os.path.join(output_path, f"{station_id}.csv")
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 打印完成信息
    print(f"文件 {station_id}.csv 已完成处理并保存至 {output_path}")

print("所有文件处理完成！")
