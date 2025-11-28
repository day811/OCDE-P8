# transformation/src/ingestion/mongo_loader.py

import json
import os
import logging
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, ConnectionFailure
import argparse
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# MONGODB LOADER
# ============================================================================

class MongoDBLoader:
    """Load normalized JSONL data into MongoDB."""
    
    def __init__(self, mongodb_uri: str, database_name: str):
        """
        Initialize MongoDB connection.
        
        Args:
            mongodb_uri: MongoDB connection string
            database_name: Target database name
        """
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.client = None
        self.db = None
    
    def connect(self) -> bool:
        """Establish connection to MongoDB."""
        try:
            logger.info(f"Connecting to MongoDB: {self.mongodb_uri.split('@')[1] if '@' in self.mongodb_uri else self.mongodb_uri}")
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            # Verify connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            logger.info(f"✓ Connected to database: {self.database_name}")
            return True
        except ConnectionFailure as e:
            logger.error(f"✗ Failed to connect to MongoDB: {e}")
            return False
    
    def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("✓ Disconnected from MongoDB")
    
    def create_collections_with_schema(self):
        """Create collections with JSON schema validation."""
        
        # Schema for weather observations
        weather_schema = {
            "bsonType": "object",
            "required": ["id_station", "dh_utc", "temperature"],
            "properties": {
                "_id": {"bsonType": "objectId"},
                "id_station": {"bsonType": "string", "description": "Station identifier"},
                "station_name": {"bsonType": ["string", "null"]},
                "latitude": {"bsonType": ["double", "null"]},
                "longitude": {"bsonType": ["double", "null"]},
                "elevation": {"bsonType": ["int", "null"]},
                "city": {"bsonType": ["string", "null"]},
                "state": {"bsonType": ["string", "null"]},
                "hardware": {"bsonType": ["string", "null"]},
                "software": {"bsonType": ["string", "null"]},
                "dh_utc": {"bsonType": "string", "description": "ISO 8601 timestamp"},
                "temperature": {"bsonType": ["double", "null"]},
                "pression": {"bsonType": ["double", "null"]},
                "humidite": {"bsonType": ["double", "null"]},
                "point_de_rosee": {"bsonType": ["double", "null"]},
                "visibilite": {"bsonType": ["double", "null"]},
                "vent_moyen": {"bsonType": ["double", "null"]},
                "vent_rafales": {"bsonType": ["double", "null"]},
                "vent_direction": {"bsonType": ["double", "null"]},
                "pluie_3h": {"bsonType": ["double", "null"]},
                "pluie_1h": {"bsonType": ["double", "null"]},
                "neige_au_sol": {"bsonType": ["double", "null"]},
                "nebulosite": {"bsonType": ["string", "null"]},
                "temps_omm": {"bsonType": ["int", "null"]},
                "_source": {"bsonType": "string"},
                "_ingestion_timestamp": {"bsonType": "string"},
                "_raw_source": {"bsonType": "string"}
            }
        }
        
        try:
            # Drop existing collection if it exists
            if "weather_observations" in self.db.list_collection_names():
                self.db["weather_observations"].drop()
                logger.info("Dropped existing collection: weather_observations")
            
            # Create collection with schema validation
            self.db.create_collection(
                "weather_observations",
                validator={"$jsonSchema": weather_schema}
            )
            logger.info("✓ Created collection: weather_observations with schema validation")
            
            # Create indexes for performance
            self.db["weather_observations"].create_index([("id_station", 1)])
            self.db["weather_observations"].create_index([("dh_utc", 1)])
            self.db["weather_observations"].create_index([("id_station", 1), ("dh_utc", 1)])
            logger.info("✓ Created indexes on weather_observations")
            
        except Exception as e:
            logger.error(f"✗ Error creating collection: {e}")
            raise
    
    def load_jsonl_file(self, jsonl_file: str, collection_name: str = "weather_observations") -> int:
        """
        Load data from JSONL file into MongoDB collection.
        
        Args:
            jsonl_file: Path to JSONL file
            collection_name: Target collection name
        
        Returns:
            Number of records inserted
        """
        collection = self.db[collection_name]
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    
                    try:
                        record = json.loads(line)
                        
                        # Upsert: update if exists, insert if not
                        # Use id_station + dh_utc as unique key
                        filter_query = {
                            "id_station": record.get("id_station"),
                            "dh_utc": record.get("dh_utc")
                        }
                        
                        result = collection.update_one(
                            filter_query,
                            {"$set": record},
                            upsert=True
                        )
                        
                        if result.upserted_id:
                            inserted_count += 1
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"  Line {line_num}: Invalid JSON - {e}")
                        error_count += 1
                    except Exception as e:
                        logger.warning(f"  Line {line_num}: Error processing record - {e}")
                        error_count += 1
            
            logger.info(f"✓ Loaded from {jsonl_file}")
            logger.info(f"  - Inserted: {inserted_count}")
            logger.info(f"  - Errors: {error_count}")
            
            return inserted_count
        
        except FileNotFoundError:
            logger.error(f"✗ File not found: {jsonl_file}")
            return 0
    
    def load_all_sources(self, input_dir: str) -> Dict[str, int]:
        """
        Load all normalized JSONL files from directory.
        
        Args:
            input_dir: Directory containing normalized JSONL files
        
        Returns:
            Dictionary with source names and record counts
        """
        results = {}
        input_path = Path(input_dir)
        
        if not input_path.exists():
            logger.error(f"✗ Input directory not found: {input_dir}")
            return results
        
        jsonl_files = list(input_path.glob("*_normalized.jsonl"))
        
        if not jsonl_files:
            logger.warning(f"⚠️  No JSONL files found in {input_dir}")
            return results
        
        logger.info(f"Found {len(jsonl_files)} JSONL file(s) to load")
        logger.info()
        
        for jsonl_file in sorted(jsonl_files):
            source_name = jsonl_file.stem.replace("_normalized", "")
            logger.info(f"Loading source: {source_name}")
            
            count = self.load_jsonl_file(str(jsonl_file))
            results[source_name] = count
        
        return results
    
    def get_statistics(self, collection_name: str = "weather_observations") -> Dict[str, Any]:
        """Get collection statistics."""
        collection = self.db[collection_name]
        
        stats = {
            'total_records': collection.count_documents({}),
            'stations': collection.distinct('id_station'),
            'date_range': {},
            'sources': collection.distinct('_source')
        }
        
        # Get date range
        date_stats = collection.aggregate([
            {
                "$group": {
                    "_id": None,
                    "min_date": {"$min": "$dh_utc"},
                    "max_date": {"$max": "$dh_utc"}
                }
            }
        ])
        
        for doc in date_stats:
            stats['date_range'] = {
                'start': doc.get('min_date'),
                'end': doc.get('max_date')
            }
        
        return stats

# ============================================================================
# DATA QUALITY CHECKER
# ============================================================================

class DataQualityChecker:
    """Check data quality in MongoDB."""
    
    def __init__(self, db):
        self.db = db
    
    def check_quality(self, collection_name: str = "weather_observations") -> Dict[str, Any]:
        """
        Perform comprehensive data quality checks.
        
        Args:
            collection_name: Collection to check
        
        Returns:
            Quality report dictionary
        """
        collection = self.db[collection_name]
        total_records = collection.count_documents({})
        
        report = {
            'total_records': total_records,
            'timestamp': datetime.utcnow().isoformat(),
            'checks': {}
        }
        
        if total_records == 0:
            logger.warning("⚠️  No records in collection")
            return report
        
        # Check 1: Missing required fields
        required_fields = ['id_station', 'dh_utc', 'temperature']
        for field in required_fields:
            missing = collection.count_documents({field: None})
            report['checks'][f'missing_{field}'] = {
                'count': missing,
                'percentage': round(missing / total_records * 100, 2)
            }
        
        # Check 2: Data type consistency
        temperature_non_numeric = collection.count_documents({
            'temperature': {'$type': 'string'}
        })
        report['checks']['temperature_non_numeric'] = {
            'count': temperature_non_numeric,
            'percentage': round(temperature_non_numeric / total_records * 100, 2)
        }
        
        # Check 3: Duplicate records (same station, same timestamp)
        duplicates = collection.aggregate([
            {
                "$group": {
                    "_id": {"station": "$id_station", "timestamp": "$dh_utc"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {"count": {"$gt": 1}}
            }
        ])
        
        duplicate_count = sum(1 for _ in duplicates)
        report['checks']['duplicate_records'] = {
            'count': duplicate_count,
            'percentage': round(duplicate_count / total_records * 100, 2) if total_records > 0 else 0
        }
        
        # Check 4: Invalid dates
        invalid_dates = collection.count_documents({
            'dh_utc': {'$regex': '^(?!\\d{4}-\\d{2}-\\d{2}T)'}
        })
        report['checks']['invalid_dates'] = {
            'count': invalid_dates,
            'percentage': round(invalid_dates / total_records * 100, 2)
        }
        
        # Check 5: Data range validation
        temp_min = collection.find_one(
            {'temperature': {'$ne': None}},
            sort=[('temperature', 1)]
        )
        temp_max = collection.find_one(
            {'temperature': {'$ne': None}},
            sort=[('temperature', -1)]
        )
        
        report['checks']['temperature_range'] = {
            'min': temp_min.get('temperature') if temp_min else None,
            'max': temp_max.get('temperature') if temp_max else None
        }
        
        return report

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Load normalized data into MongoDB')
    parser.add_argument(
        '--mongodb-uri',
        default=os.getenv('MONGODB_URI', 'mongodb://admin:password@localhost:27017'),
        help='MongoDB connection URI'
    )
    parser.add_argument(
        '--database',
        default=os.getenv('DATABASE_NAME', 'greencoop_forecast'),
        help='Target database name'
    )
    parser.add_argument(
        '--input-dir',
        default=os.getenv('INPUT_DIR', 'data/clean'),
        help='Directory containing normalized JSONL files'
    )
    parser.add_argument(
        '--skip-schema',
        action='store_true',
        help='Skip schema creation (reuse existing collection)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("MONGODB DATA INGESTION")
    logger.info("=" * 70)
    logger.info()
    
    try:
        # Initialize loader
        loader = MongoDBLoader(args.mongodb_uri, args.database)
        
        # Connect
        if not loader.connect():
            sys.exit(1)
        
        # Create schema
        if not args.skip_schema:
            loader.create_collections_with_schema()
            logger.info()
        
        # Load data
        logger.info("Loading normalized data...")
        logger.info()
        results = loader.load_all_sources(args.input_dir)
        logger.info()
        
        # Get statistics
        stats = loader.get_statistics()
        logger.info("=" * 70)
        logger.info("INGESTION COMPLETE - Statistics")
        logger.info("=" * 70)
        logger.info(f"Total records: {stats['total_records']}")
        logger.info(f"Stations: {len(stats['stations'])} - {', '.join(sorted(stats['stations']))}")
        logger.info(f"Sources: {', '.join(stats['sources'])}")
        logger.info(f"Date range: {stats['date_range'].get('start')} to {stats['date_range'].get('end')}")
        logger.info()
        
        # Data quality check
        logger.info("Running data quality checks...")
        logger.info()
        quality_checker = DataQualityChecker(loader.db)
        quality_report = quality_checker.check_quality()
        
        logger.info("=" * 70)
        logger.info("DATA QUALITY REPORT")
        logger.info("=" * 70)
        for check_name, check_result in quality_report['checks'].items():
            if isinstance(check_result, dict) and 'percentage' in check_result:
                logger.info(f"{check_name}: {check_result['count']} ({check_result['percentage']}%)")
            else:
                logger.info(f"{check_name}: {check_result}")
        logger.info()
        
        # Disconnect
        loader.disconnect()
        
        logger.info("✅ SUCCESS")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
