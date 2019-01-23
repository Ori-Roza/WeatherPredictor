from data_collector import DataCollector

def create_datasets():
    dataset = [["date", "min_temp", "max_temp", "avg_temp", "season_association"]]
    collector = DataCollector()
    collector.fetch_data_to_db()
    for temp in collector.db.collection.find().sort("date"):
        dataset.append([temp["date"], float(temp["min_temp"]), float(temp["max_temp"]), float(temp["avg_temp"]),
                        temp["season_association"]])
    return dataset
