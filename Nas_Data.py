import os
import re
import pandas as pd
import datetime
import shutil
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.simplefilter(action='ignore', category=FutureWarning)


# 重命名字典
rename_dict_power = {
    "time": "Time", "TIME": "Time", "时间.1": "Time",
    "Power(mw)": "rtPower(MW)", "Power(MW)": "rtPower(MW)", "Power（MW）": "rtPower(MW)",
    "Power（MW)": "rtPower(MW)", "Power(Mw)": "rtPower(MW)", "power（mw）": "rtPower(MW)",
    "亮马光伏电站": "rtPower(MW)", "Power(WM)": "rtPower(MW)",
    "焦作.亮马光伏电站亮马光伏有功总加值": "rtPower(MW)", "POWER(mv)": "rtPower(MW)",
    "QX2": "rtPower(MW)", "POWER(MW)": "rtPower(MW)", "Power（mw)": "rtPower(MW)",
    "power(mw)": "rtPower(MW)", "Power（mw）": "rtPower(MW)", "总有功": "rtPower(MW)",
    "焦作.庞冯营光伏电站庞冯营有功总加值Power(MW)": "rtPower(MW)",
}

rename_dict_radiation = {
    "TIME": "Time", "TIME ": "Time", "Radiation(w/m2)": "Radiation",
    "RADIATION(w/m2)": "Radiation", "RADIATION(w/m²)": "Radiation",
    "Radiation(w/m²）": "Radiation", "Radiation（w/m²)": "Radiation",
    "RADIATION(W/M2)": "Radiation", "Gerneral_Radiation": "Radiation",
    "Radiation（w/m2）": "Radiation", "GERNERAL_RADIATION": "Radiation",
    "Power(MW)": "Radiation", "XQ2": "Radiation",
}

# 修改列名
rename_final_columns = {
    "rtPower(MW)": "Power_Nas",
    "RADIATION": "Radiation_Nas",
    "Radiation": "Radiation_Nas",
}

# 使用 os.walk 遍历路径并获取所有文件
def get_all_files(root_directory):
    file_list = []
    for dirpath, _, filenames in os.walk(root_directory):
        for file in filenames:
            if file.endswith((".xlsx", ".xls")):
                file_path = os.path.join(dirpath, file)
                file_list.append(file_path)
    return file_list

# 数据处理函数
def process_files(path_list, prefix, rename_dict, output_folder, data_type, error_log):
    os.makedirs(output_folder, exist_ok=True)
    error_nwp_list = []
    df = pd.DataFrame()

    for file_path in path_list:
        basename = os.path.basename(file_path)
        if basename.startswith(prefix) and f"_{data_type}_" in basename:
            try:
                tmp = pd.read_excel(file_path)

                # 检查并解析时间列
                if "TIME" in tmp.columns and "Time" in tmp.columns:
                    tmp.drop(["TIME"], axis=1, inplace=True)
                tmp.rename(columns=rename_dict, inplace=True)

                # 检查时间列是否有效
                if "Time" not in tmp.columns:
                    print(f"文件 {file_path} 缺少时间列，跳过处理")
                    error_nwp_list.append(prefix)
                    continue
                tmp['Time'] = pd.to_datetime(tmp['Time'], errors='coerce')
                tmp = tmp.dropna(subset=['Time'])  # 删除时间无效的行
                tmp.rename(columns=rename_final_columns, inplace=True)

                # 如果列缺失，添加空列
                if "Power_Nas" not in tmp.columns and data_type == "Power":
                    tmp["Power_Nas"] = None
                if "Radiation_Nas" not in tmp.columns and data_type == "Radiation":
                    tmp["Radiation_Nas"] = None

                tmp.set_index("Time", inplace=True)
                df = pd.concat([df, tmp]).sort_values(by='Time')
            except Exception as e:
                error_nwp_list.append(prefix)
                print(f"Error processing file {basename}: {e}")

    if not df.empty:
        start_time = datetime.datetime.strftime(df.index[0].date(), "%Y%m%d")
        end_time = datetime.datetime.strftime(df.index[-1].date(), "%Y%m%d")
        output_file = os.path.join(output_folder, f"{prefix}_{data_type}_{start_time}_{end_time}.csv")
        df.to_csv(output_file)
    else:
        error_nwp_list.append(prefix)

    error_log.extend(error_nwp_list)
    return error_nwp_list

def extract_station_number(filename):
    match = re.search(r'NARI-(\d+)-', filename)
    return match.group(1) if match else None

def merge_station_files(power_folder, radiation_folder, output_folder, error_log):
    os.makedirs(output_folder, exist_ok=True)
    power_files = {extract_station_number(f): os.path.join(power_folder, f) for f in os.listdir(power_folder) if f.endswith('.csv')}
    radiation_files = {extract_station_number(f): os.path.join(radiation_folder, f) for f in os.listdir(radiation_folder) if f.endswith('.csv')}

    for station_number, power_file in power_files.items():
        radiation_file = radiation_files.get(station_number)
        try:
            # 读取Power文件
            power_data = pd.read_csv(power_file, index_col='Time', parse_dates=True)
            if "Power_Nas" not in power_data.columns:
                power_data["Power_Nas"] = None  # 确保Power_Nas列存在

            # 读取Radiation文件
            if radiation_file:
                radiation_data = pd.read_csv(radiation_file, index_col='Time', parse_dates=True)

                # 检查是否存在`Radiation_Nas`列，如果不存在则创建空列
                if 'Radiation_Nas' not in radiation_data.columns:
                    radiation_data['Radiation_Nas'] = None
                radiation_data = radiation_data[['Radiation_Nas']]  # 只保留`Radiation_Nas`列
            else:
                # 如果辐射文件不存在，创建一个空的DataFrame
                radiation_data = pd.DataFrame(index=power_data.index)
                radiation_data["Radiation_Nas"] = None

            # 合并数据
            merged_data = pd.merge(power_data, radiation_data, left_index=True, right_index=True, how='outer')

            # 保存合并结果
            output_file = os.path.join(output_folder, f"{station_number}.csv")
            merged_data.to_csv(output_file)
        except Exception as e:
            error_log.append(station_number)
            print(f"Error merging station {station_number}: {e}")

def clean_and_save_final(source_folder, target_folder, start_time, time_freq="5min"):
    os.makedirs(target_folder, exist_ok=True)
    csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]

    if not csv_files:
        return

    # 合并重复时间索引
    for csv_file in csv_files:
        file_path = os.path.join(source_folder, csv_file)
        try:
            data = pd.read_csv(file_path, index_col='Time', parse_dates=True)

            # 如果数据为空，跳过处理
            if data.empty:
                continue

            # 处理重复时间索引
            data = data[~data.index.duplicated(keep='last')]

            # 创建完整时间序列
            full_time_index = pd.date_range(start=start_time, end=data.index.max(), freq=time_freq)
            data = data.reindex(full_time_index)

            # 重置索引并保存
            data.reset_index(inplace=True)
            data.rename(columns={"index": "Time"}, inplace=True)
            output_file_path = os.path.join(target_folder, csv_file)
            data.to_csv(output_file_path, index=False)
            print(f"已保存文件：{output_file_path}")
        except Exception as e:
            print(f"处理文件 {csv_file} 失败: {e}")

def write_error_log(error_log, log_file):
    with open(log_file, "w") as f:
        for error in error_log:
            f.write(f"{error}\n")
    print(f"错误场站信息已保存到: {log_file}")

# 主流程
backup_folder = r"D:\新能源预测小组\Project\concat\data\backup"
power_folder = r"D:\新能源预测小组\Project\concat\data\backup\Power-5.5"
radiation_folder = r"D:\新能源预测小组\Project\concat\data\backup\Radiation-5.5"
merged_folder = r"D:\新能源预测小组\Project\concat\data\backup\merged-5.5"
cleaned_folder = r"D:\新能源预测小组\Project\concat\data\Nas"
log_file = r"D:\新能源预测小组\Project\concat\data\error_log.txt"

INFO = pd.read_excel("station_info.xlsx").set_index("NwpId")
root_directory = r"\\192.168.5.5\homes\projectControl\0000 给XBY数据\0001 光伏反馈"

# 获取所有匹配的文件路径
path_list = get_all_files(root_directory)

# 确保所有输出文件夹存在
os.makedirs(power_folder, exist_ok=True)
os.makedirs(radiation_folder, exist_ok=True)
os.makedirs(merged_folder, exist_ok=True)
os.makedirs(cleaned_folder, exist_ok=True)

error_log = []

# 对每个场站进行数据处理
for nwp in INFO.index:
    prefix = INFO.loc[nwp, "天气预报前缀"]
    process_files(path_list, prefix, rename_dict_power, power_folder, "Power", error_log)
    process_files(path_list, prefix, rename_dict_radiation, radiation_folder, "Radiation", error_log)

merge_station_files(power_folder, radiation_folder, merged_folder, error_log)

# 执行清理
start_time = pd.Timestamp("2021-01-01 00:00")
clean_and_save_final(merged_folder, cleaned_folder, start_time)

# 写入日志文件
write_error_log(error_log, log_file)

# 删除过程处理文件
backup_folder_path = r"D:\新能源预测小组\Project\concat\data\backup"
if os.path.exists(backup_folder_path):
    shutil.rmtree(backup_folder_path)
    print(f"已成功删除文件夹: {backup_folder_path}")
else:
    print(f"文件夹不存在: {backup_folder_path}")

print(f"所有文件已处理完成，并保存到 {cleaned_folder} 文件夹中！")

