import pandas as pd
import numpy as np
import os
import datetime
import pymongo
from functools import reduce
from datetime import datetime, timedelta

column_name_map = {
    "WIND": {
        "rtLoad": "实测功率",
        "forecastWeather": "预测风速风向",
        "rtTower": "实测风速实测风向",
        "forecastShort": "预测功率",
    },
    "SOLAR": {
        "rtLoad": "实测功率",
        "forecastWeather": "预测辐照度",
        "rtTower": "实测辐照度",
        "forecastShort": "预测功率",
    }
}

class MongoDBFetcher:
    def __init__(self, db_name="nwpc", layer=80, log=False):
        username = os.getenv("MONGO_USERNAME", "nari")
        password = os.getenv("MONGO_PASSWORD", "nari0755")
        host = os.getenv("MONGO_HOST", "192.168.5.8")
        port = os.getenv("MONGO_PORT", "27017")
        
        mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/"
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.layer = layer
        if log:
            self._log_collections_info(db_name)

    def _log_collections_info(self, db_name):
        db_list = self.client.list_database_names()
        collection_list = self.db.list_collection_names()
        print(f"Database List: {db_list}")
        print(f"Collection List for {db_name}: {collection_list}")

    def _get_collection_data(self, collection_name, station_id, col_name, source="FINAL", days=7, end_time=None, add_source_suffix=False):
        end_time = end_time or (datetime.today() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=days)
        
        if col_name in ["\u9884\u6d4b\u98ce\u901f\u98ce\u5411", "\u5b9e\u6d4b\u98ce\u901f\u5b9e\u6d4b\u98ce\u5411"]:
            weather_types = ["WIND_SPEED", "WIND_DIR"]
            data_frames = []
            for weather_type in weather_types:
                col_name_temp = "\u5b9e\u6d4b\u98ce\u901f" if weather_type == "WIND_SPEED" else "\u5b9e\u6d4b\u98ce\u5411"
                if col_name == "\u9884\u6d4b\u98ce\u901f\u98ce\u5411":
                    col_name_temp = "\u9884\u6d4b\u98ce\u901f" if weather_type == "WIND_SPEED" else "\u9884\u6d4b\u98ce\u5411"
                query = self._build_query(collection_name, station_id, col_name_temp, source, start_time, end_time, weather_type)
                cursor = self.db[collection_name].find(query).sort("ybDate", pymongo.ASCENDING)
                df = pd.DataFrame(list(cursor))
                df = df.drop_duplicates(subset='ybDate')

                if df.empty:
                    print(f"No data found for station {station_id} and column {col_name_temp} with weather type {weather_type}")
                    continue
                formatted_df = self._format_dataframe(df, col_name_temp, source, add_source_suffix)
                data_frames.append(formatted_df)
            if data_frames:
                return reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how="outer"), data_frames)
            else:
                return pd.DataFrame()
        else:
            query = self._build_query(collection_name, station_id, col_name, source, start_time, end_time)
            cursor = self.db[collection_name].find(query).sort("ybDate", pymongo.ASCENDING)
            df = pd.DataFrame(list(cursor))
            df = df.drop_duplicates(subset='ybDate')

            if df.empty:
                print(f"No data found for station {station_id} and column {col_name}")
                return pd.DataFrame()
            return self._format_dataframe(df, col_name, source, add_source_suffix)

    def _build_query(self, collection_name, station_id, col_name, source, start_time, end_time, weather_type=None):
        query = {"stationId": station_id, "ybDate": {"$gte": start_time, "$lte": end_time}}
        if "forecast" in collection_name:
            query["dataSource"] = source
        if collection_name in ["forecastWeather", "rtTower"]:
            if weather_type:
                query["weatherType"] = weather_type
            else:
                query["weatherType"] = "ALL_RADIATION" if "\u8f90\u7167\u5ea6" in col_name else "WIND_SPEED"
            if "WIND_SPEED" == query["weatherType"] or weather_type == "WIND_DIR":
                query["layer"] = self.layer
        return query

    def _format_dataframe(self, df, col_name, source, add_source_suffix):
        def point_to_time(point):
            point_index = int(point.replace('point', '')) - 1
            hours = (point_index * 5) // 60
            minutes = (point_index * 5) % 60
            return f'{hours:02d}:{minutes:02d}' 

        df = df.drop(columns=["_id", "_class", "stationId"], errors="ignore")
        df["TIME"] = pd.to_datetime(df["ybDate"]) + pd.DateOffset(days=1)
        df = df.drop(columns=["ybDate"]).set_index("TIME")
        df = df[[f"point{i}" for i in range(1, 289)]].stack().reset_index()
        df["level_1"] = df['level_1'].apply(point_to_time)
        df["TIME"] = df["TIME"].dt.date.astype(str) + " " + df["level_1"]
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = df.drop(columns=["level_1"]).set_index("TIME")
        if add_source_suffix:
            col_name = f"{col_name}_{source}"
        df = df.rename(columns={0: col_name})
        return df.shift(1).dropna()

    def get_station_data(self, station_id, 
                         days=15, 
                         only_rt=False,
                         need_weather=False,
                         end_time=None,
                         db_list=None,
                         sources=["FINAL"],
                         station_type="WIND"):
        collections = ['rtLoad']
        if not db_list:
            if not only_rt:
                collections += ['forecastShort']
            if need_weather:
                collections += ['forecastWeather', 'rtTower']
            if end_time:
                end_time = pd.to_datetime(end_time)
        else:
            collections = db_list
        data_frames = []
        add_source_suffix = len(sources) > 1

        for collection in collections:
            for source in sources:
                col_name = self._get_column_name_based_on_context(collection, station_type)
                print(f'reading {col_name} data from source {source}')

                if col_name:
                    df = self._get_collection_data(collection, station_id, col_name, source=source, days=days, end_time=end_time, add_source_suffix=add_source_suffix)
                    if not df.empty:
                        data_frames.append(df)

        if not data_frames:
            return pd.DataFrame()

        merged_df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how="outer"), data_frames)
        return merged_df.sort_index().replace(-99, np.nan).round(4)

    def _get_column_name_based_on_context(self, collection_name, station_type):
        return column_name_map.get(station_type, {}).get(collection_name)


if __name__ == "__main__":
    INFO = pd.read_excel("station_info.xlsx").set_index("NwpId")
    for nwp in INFO.index:
        f = MongoDBFetcher(layer=80)
        df = f.get_station_data(nwp, need_weather=True, db_list=["rtLoad", "rtTower"], station_type="SOLAR", days=2000, sources=["FINAL"])
        output_path = r"D:\\新能源预测小组\\Project\\concat\\data\\DB_Download"

        if df.empty or df.columns.empty:
            print(f"No data found for {nwp}, generating default data")
            start_date = pd.to_datetime("2021-01-01")
            end_date = pd.to_datetime("today")
            time_index = pd.date_range(start=start_date, end=end_date, freq="5min")
            df = pd.DataFrame(index=time_index, columns=["Power_DB", "Radiation_DB"])
        
        column_rename_map = {
            "\u5b9e\u6d4b\u529f\u7387": "Power_DB",
            "\u5b9e\u6d4b\u8f90\u7167\u5ea6": "Radiation_DB"
        }
        df = df.rename(columns=column_rename_map)

        df.index.name = "Time"

        df.to_csv(f'{output_path}/{nwp}.csv', index=True, encoding='utf-8')
        print(f"文件{nwp}已处理完毕")

    source_folder = r"D:\\新能源预测小组\\Project\\concat\\data\\DB_Download"
    target_folder = r"D:\\新能源预测小组\\Project\\concat\\data\\DB"

    os.makedirs(target_folder, exist_ok=True)
    start_time = pd.Timestamp("2021-01-01 00:00")
    time_freq = "5min"

    for filename in os.listdir(source_folder):
        if filename.endswith(".csv"):
            source_file_path = os.path.join(source_folder, filename)
            target_file_path = os.path.join(target_folder, filename)
            
            df = pd.read_csv(source_file_path)
            df.rename(columns={df.columns[0]: "Time"}, inplace=True)
            try:
                df["Time"] = pd.to_datetime(df["Time"], format="%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                df["Time"] = pd.to_datetime(df["Time"], format='mixed', errors='coerce')
            
            df.set_index("Time", inplace=True)
            df = df[df.index >= start_time]
            
            if df.empty or df.index.max() is pd.NaT:
                print(f"文件 {filename} 时间索引为空，已跳过。")
                continue
            
            df = df[~df.index.duplicated(keep='last')]
            full_time_index = pd.date_range(start=start_time, end=df.index.max(), freq=time_freq)
            df = df.reindex(full_time_index)
            
            if 'Power_DB' in df.columns and 'Radiation_DB' in df.columns:
                df = df[['Power_DB', 'Radiation_DB']]      
            
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Time"}, inplace=True)

            df.to_csv(target_file_path, index=False)

    print(f"所有文件已处理完成，并保存到 {target_folder} 文件夹中！")
