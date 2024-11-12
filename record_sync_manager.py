from pymongo import MongoClient
import pika
from logging import info
from json import dumps


class MissingRecordHandler:
    """Handles missing records from primary and secondary data sources"""

    def __init__(self, primary_data_uri, secondary_data_uri, source_collection, target_collection, url_collection, current_date):
        # MongoDB Clients
        self.primary_data = MongoClient(primary_data_uri)
        self.secondary_data = MongoClient(secondary_data_uri)
        self.source_collection = source_collection
        self.target_collection = target_collection
        self.url_collection = url_collection
        self.current_date = current_date

    def start(self):
        """Database insertion function for handling missing records"""
        
        collection = self.primary_data.db[self.source_collection]
        site_name = self.source_collection.split("_")[0].strip()
        url_collection = self.primary_data.db[self.url_collection]
        master_collection = self.secondary_data.db[self.target_collection]
        master_insert_info = self.primary_data.db["master_urls_info"]

        new_record_ids = collection.distinct("unique_id")
        if self.current_date == 'SpecialDay':
            master_record_ids = master_collection.distinct("unique_id")
        else:
            master_record_ids = master_collection.find({"status": 200}).distinct("unique_id")
        
        missing_records = list(set(master_record_ids) - set(new_record_ids))
        documents = master_collection.find({"unique_id": {"$in": missing_records}})
        
        records_to_insert = []
        for document in documents:
            record_url = document.get("record_url")
            unique_id = document.get("unique_id")
            records_to_insert.append({"record_url": record_url, "unique_id": unique_id})
        
        info(f"Missing record count: {len(records_to_insert)}")
        
        try:
            master_insert_info.insert_one({
                "site": site_name,
                "info": {
                    "missing_count": len(missing_records),
                }
            })
        except:
            pass
        
        if site_name == "site1":
            for record in records_to_insert:
                try:
                    url_collection.insert_one({
                        "unique_id": record.get("unique_id"),
                        "url": record.get("record_url"),
                        "category": "MasterDB",
                        "additional_field": ""
                    })
                except Exception as e:
                    info(f"Failed to insert URLs: {e}")
                    pass   
        else:
            for record in records_to_insert:
                try:
                    url_collection.insert_one({
                        "unique_id": record.get("unique_id"),
                        "url": record.get("record_url"),
                    })
                except Exception as e:
                    info(f"Failed to insert URLs: {e}")
                    pass

    def close(self):
        info(">>>> Missing record handling completed <<<<")


class QueueRecordHandler:
    """Handles queuing missing records for further processing"""

    def __init__(self, primary_data_uri, secondary_data_uri, source_collection, target_collection, current_date, rabbitmq_uri):
        # MongoDB Clients
        self.primary_data = MongoClient(primary_data_uri)
        self.secondary_data = MongoClient(secondary_data_uri)
        self.source_collection = source_collection
        self.target_collection = target_collection
        self.current_date = current_date

        # RabbitMQ Connection
        self.rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_uri))
        self.rabbitmq_channel = self.rabbitmq_connection.channel()

    def start(self):
        """Queue insertion function for missing records"""
        
        collection = self.primary_data.db[self.source_collection]
        site_name = self.source_collection.split("_")[0].strip()
        master_collection = self.secondary_data.db[self.target_collection]
        master_insert_info = self.primary_data.db["master_urls_info"]

        new_record_ids = collection.distinct("unique_id")
        if self.current_date == 'SpecialDay':
            master_record_ids = master_collection.distinct("unique_id")
        else:
            master_record_ids = master_collection.find({"status": 200}).distinct("unique_id")
        
        missing_records = list(set(master_record_ids) - set(new_record_ids))
        documents = master_collection.find({"unique_id": {"$in": missing_records}})
        
        records_to_enqueue = []
        for document in documents:
            record_url = document.get("record_url")
            unique_id = document.get("unique_id")
            records_to_enqueue.append({"record_url": record_url, "unique_id": unique_id})
        
        info(f"Missing record count for queuing: {len(records_to_enqueue)}")
        
        try:
            master_insert_info.insert_one({
                "site": site_name,
                "info": {
                    "missing_count": len(missing_records),
                }
            })
        except:
            pass
        
        for record in records_to_enqueue:
            try:
                self.rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key='queue_name',
                    body=dumps(record)
                )
            except Exception as e:
                info(f"Failed to queue record: {e}")
                pass

    def close(self):
        self.rabbitmq_connection.close()
        info(">>>> Queuing process completed <<<<")


class UpdateMasterRecord:
    """Handles updating records in the master database"""

    def __init__(self, primary_data_uri, secondary_data_uri, source_collection, target_collection, run_id, rabbitmq_uri, **kwargs):
        # MongoDB Clients
        self.primary_data = MongoClient(primary_data_uri)
        self.secondary_data = MongoClient(secondary_data_uri)
        self.source_collection = source_collection
        self.target_collection = target_collection
        self.run_id = run_id
        self.additional_args = kwargs if kwargs.keys() else {}

        # RabbitMQ Connection (for any future queuing needs)
        self.rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_uri))
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        info(self.additional_args)

    def start(self):
        """Updating master database records"""
        
        collection = self.primary_data.db[self.source_collection]
        site_name = self.source_collection.split("_")[0].strip()
        master_collection = self.secondary_data.db[self.target_collection]
        master_insert_info = self.primary_data.db["master_urls_info"]
        
        records_updated, records_inserted, records_404 = [], [], []
        
        for item in collection.find(no_cursor_timeout=True):
            unique_id = item.get('unique_id')
            record_url = item.get("record_url")
            master_record = master_collection.find_one({"unique_id": unique_id})
            
            update_data = {"status": 200, "run_id": self.run_id, "record_url": record_url}
            if master_record:
                for key, value in self.additional_args.items():
                    update_data[key] = item.get(key, "")
                
                master_collection.update_one({"unique_id": unique_id}, {"$set": update_data})
                records_updated.append(unique_id)
            else:
                insert_data = {"unique_id": unique_id, "record_url": record_url, "status": 200, "run_id": self.run_id}
                for key, value in self.additional_args.items():
                    insert_data[key] = item.get(key, "")

                master_collection.insert_one(insert_data)
                records_inserted.append(unique_id)

        for missing_record in master_collection.find({"unique_id": {"$nin": collection.distinct("unique_id")}}):
            master_collection.update_one({"unique_id": missing_record["unique_id"]}, {"$set": {"status": 404}})
            records_404.append(missing_record["unique_id"])
        
        master_insert_info.update_one(
            {"site": site_name},
            {"$set": {
                "info.updated": len(records_updated),
                "info.new_records": len(records_inserted),
                "info.not_found": len(records_404),
                "info.total_records": master_collection.estimated_document_count()
            }}
        )
        info(f"Records updated: {len(records_updated)}")
        info(f"Records inserted: {len(records_inserted)}")
        info(f"Records marked as 404: {len(records_404)}")

    def close(self):
        self.rabbitmq_connection.close()
        info(">>>> Update and Insert process completed <<<<")
