# src/ingestion/mongo_loader.py

"""
MongoDB Data Ingestion Pipeline - Normalized Schema
Loads JSONL data into MongoDB with 3 normalized collections:
- stations: Station metadata
- observations: Weather observations
- schema_metadata: Data field definitions from output_metadata

Supports reading from S3 or local filesystem.
"""

import json
import os
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timezone
import argparse
from abc import ABC, abstractmethod

import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, ConnectionFailure, OperationFailure

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/ingestion.log', mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

COLLECTION_STATIONS = 'stations'
COLLECTION_OBSERVATIONS = 'observations'
COLLECTION_SCHEMA_METADATA = 'schema_metadata'

# ============================================================================
# DATA SOURCE ABSTRACTION
# ============================================================================

class DataSource(ABC):
    """Abstract base for reading from different sources."""
    
    @abstractmethod
    def read_file(self, path: str) -> str:
        """Read and return file content."""
        pass
    
    @abstractmethod
    def list_files(self, pattern: str) -> List[str]:
        """List files matching pattern."""
        pass


class LocalDataSource(DataSource):
    """Read from local filesystem."""
    
    def read_file(self, path: str) -> str:
        """Read local file."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f'File not found: {path}')
            raise
    
    def list_files(self, pattern: str) -> List[str]:
        """List local files matching pattern."""
        base_path = Path(pattern).parent
        glob_pattern = Path(pattern).name
        
        if not base_path.exists():
            logger.warning(f'Directory not found: {base_path}')
            return []
        
        return [str(f) for f in base_path.glob(glob_pattern)]


class S3DataSource(DataSource):
    """Read from AWS S3."""
    
    def __init__(self, bucket: str, region: str = 'eu-west-3'):
        """Initialize S3 client with explicit credentials."""
        if not HAS_BOTO3:
            raise ImportError('boto3 required for S3 support. Install with: pip install boto3')
        
        # Check credentials
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not access_key or not secret_key:
            raise ValueError(
                'AWS credentials required for S3 access:\n'
                '  - AWS_ACCESS_KEY_ID\n'
                '  - AWS_SECRET_ACCESS_KEY'
            )
        
        # Initialize S3 client with explicit credentials
        self.bucket = bucket
        self.s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
    
    def read_file(self, s3_path: str) -> str:
        """Read file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_path)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f'Failed to read from S3: {s3_path} - {e}')
            raise
    
    def list_files(self, prefix: str) -> List[str]:
        """List files in S3 with prefix."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if 'Contents' not in response:
                return []
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.jsonl')]
        except Exception as e:
            logger.error(f'Failed to list S3 files: {e}')
            return []


# ============================================================================
# DATA SOURCE FACTORY
# ============================================================================

def get_data_source(local_storage: str, s3_bucket: Optional[str] = None) -> DataSource:
    """
    Determine data source based on input path.
    
    Args:
        local_storage: Local path or S3 prefix
        s3_bucket: S3 bucket name (if using S3)
    
    Returns:
        Appropriate DataSource implementation
    """
    if s3_bucket:
        logger.info(f'Using S3 data source: s3://{s3_bucket}/{local_storage}')
        return S3DataSource(s3_bucket)
    else:
        logger.info(f'Using local data source: {local_storage}')
        return LocalDataSource()


# ============================================================================
# MONGODB CONNECTION & INITIALIZATION
# ============================================================================

class MongoDBConnection:
    """Manage MongoDB connection."""
    
    def __init__(self, mongodb_uri: str, database_name: str):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.client: Optional[MongoClient] = None
        self.db = None
    
    def connect(self) -> bool:
        """Establish connection to MongoDB."""
        try:
            if '@' in self.mongodb_uri:
                masked_uri = self.mongodb_uri.split('@')[1]
            else:
                masked_uri = self.mongodb_uri
            
            logger.info(f'Connecting to MongoDB: {masked_uri}')
            
            self.client = MongoClient(
                self.mongodb_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                retryWrites=True
            )
            
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            
            logger.info(f'✓ Connected to database: {self.database_name}')
            return True
        
        except ConnectionFailure as e:
            logger.error(f'✗ Connection failed: {e}')
            return False
    
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info('✓ Disconnected from MongoDB')
    
    def get_database(self):
        """Get database object."""
        return self.db


# ============================================================================
# SCHEMA MANAGEMENT
# ============================================================================

class SchemaManager:
    """Manage MongoDB collections and indexes."""
    
    def __init__(self, db):
        self.db = db
    
    def create_collections(self, drop_existing: bool = False) -> bool:
        """Create collections with schema validation."""
        try:
            # Drop existing collections if requested
            if drop_existing:
                for collection_name in [COLLECTION_STATIONS, COLLECTION_OBSERVATIONS, COLLECTION_SCHEMA_METADATA]:
                    if collection_name in self.db.list_collection_names():
                        self.db[collection_name].drop()
                        logger.info(f'Dropped existing collection: {collection_name}')
            
            # Create stations collection
            if COLLECTION_STATIONS not in self.db.list_collection_names():
                self.db.create_collection(COLLECTION_STATIONS)
                logger.info(f'✓ Created collection: {COLLECTION_STATIONS}')
            
            # Create observations collection with schema validation
            if COLLECTION_OBSERVATIONS not in self.db.list_collection_names():
                observations_schema = {
                    'bsonType': 'object',
                    'required': ['id_station', 'dh_utc'],
                    'properties': {
                        '_id': {'bsonType': 'objectId'},
                        'id_station': {'bsonType': 'string'},
                        'dh_utc': {'bsonType': 'string'},
                        # Measurement fields (all optional, can be null)
                        'temperature': {'bsonType': ['double', 'null']},
                        'pression': {'bsonType': ['double', 'null']},
                        'humidite': {'bsonType': ['double', 'null']},
                        'point_de_rosee': {'bsonType': ['double', 'null']},
                        'visibilite': {'bsonType': ['double', 'null']},
                        'vent_moyen': {'bsonType': ['double', 'null']},
                        'vent_rafales': {'bsonType': ['double', 'null']},
                        'vent_direction': {'bsonType': ['double', 'null']},
                        'pluie_3h': {'bsonType': ['double', 'null']},
                        'pluie_1h': {'bsonType': ['double', 'null']},
                        'neige_au_sol': {'bsonType': ['double', 'null']},
                        'nebulosite': {'bsonType': ['string', 'null']},
                        'temps_omm': {'bsonType': ['int', 'null']},
                        '_source': {'bsonType': 'string'},
                        '_ingestion_timestamp': {'bsonType': 'string'}
                    }
                }
                
                self.db.create_collection(
                    COLLECTION_OBSERVATIONS,
                    validator={'$jsonSchema': observations_schema}
                )
                logger.info(f'✓ Created collection: {COLLECTION_OBSERVATIONS} with schema validation')
            
            # Create schema_metadata collection
            if COLLECTION_SCHEMA_METADATA not in self.db.list_collection_names():
                self.db.create_collection(COLLECTION_SCHEMA_METADATA)
                logger.info(f'✓ Created collection: {COLLECTION_SCHEMA_METADATA}')
            
            # Create indexes
            self._create_indexes()
            
            return True
        
        except OperationFailure as e:
            logger.error(f'✗ Schema creation failed: {e}')
            return False
    
    def _create_indexes(self) -> None:
        """Create indexes for query optimization."""
        # Stations indexes
        self.db[COLLECTION_STATIONS].create_index([('id_station', ASCENDING)], unique=True, name='idx_station_unique')
        self.db[COLLECTION_STATIONS].create_index([('city', ASCENDING)], name='idx_city')
        logger.info('✓ Created indexes on stations')
        
        # Observations indexes
        self.db[COLLECTION_OBSERVATIONS].create_index([('id_station', ASCENDING)], name='idx_obs_station')
        self.db[COLLECTION_OBSERVATIONS].create_index([('dh_utc', DESCENDING)], name='idx_obs_timestamp')
        self.db[COLLECTION_OBSERVATIONS].create_index(
            [('id_station', ASCENDING), ('dh_utc', DESCENDING)],
            name='idx_obs_station_time'
        )
        logger.info('✓ Created indexes on observations')
        
        # Schema metadata indexes
        self.db[COLLECTION_SCHEMA_METADATA].create_index([('field_name', ASCENDING)], unique=True, name='idx_field_unique')
        logger.info('✓ Created indexes on schema_metadata')


# ============================================================================
# DATA VALIDATION
# ============================================================================

class DataValidator:
    """Validate data before MongoDB insertion."""
    
    @staticmethod
    def validate_observation(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate observation record."""
        # Required fields
        if not record.get('id_station'):
            return False, 'Missing id_station'
        
        if not record.get('dh_utc'):
            return False, 'Missing dh_utc'
        
        # Timestamp format check
        timestamp = record.get('dh_utc', '')
        if not isinstance(timestamp, str) or 'T' not in timestamp or 'Z' not in timestamp:
            return False, f'Invalid ISO 8601 timestamp: {timestamp}'
        
        # At least one measurement required
        measurement_fields = [
            'temperature', 'pression', 'humidite', 'vent_moyen',
            'vent_rafales', 'pluie_1h', 'visibilite'
        ]
        has_measurement = any(record.get(field) is not None for field in measurement_fields)
        if not has_measurement:
            return False, 'No measurement values present'
        
        return True, None
    
    @staticmethod
    def validate_station(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate station record."""
        if not record.get('id_station'):
            return False, 'Missing id_station'
        
        return True, None


# ============================================================================
# DATA LOADING
# ============================================================================

class DataLoader:
    """Load data into MongoDB collections."""
    
    def __init__(self, db, validator: DataValidator):
        self.db = db
        self.validator = validator
        self.stations_cache = set()  # Track loaded stations
    
    def load_jsonl_data(self, content: str, source_name: str) -> Dict[str, int]:
        """
        Load JSONL data from string content.
        
        Args:
            content: JSONL content
            source_name: Name of data source
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'stations_inserted': 0,
            'stations_updated': 0,
            'observations_inserted': 0,
            'observations_updated': 0,
            'schema_records_inserted': 0,
            'skipped': 0,
            'errors': 0
        }
        
        lines = content.strip().split('\n')
        
        for line_num, line in enumerate(lines, 1):
            if not line.strip():
                continue
            
            try:
                data = json.loads(line)
                
                # Handle bulk structure (stations + hourly)
                if 'stations' in data and 'hourly' in data:
                    # Process stations
                    for station in data.get('stations', []):
                        result = self._upsert_station(station)
                        if result == 'inserted':
                            stats['stations_inserted'] += 1
                        elif result == 'updated':
                            stats['stations_updated'] += 1
                    
                    # Process observations per station
                    for station_id, observations in data.get('hourly', {}).items():
                        if isinstance(observations, list):
                            for obs in observations:
                                result = self._insert_observation(obs, source_name)
                                if result == 'inserted':
                                    stats['observations_inserted'] += 1
                                elif result == 'updated':
                                    stats['observations_updated'] += 1
                                elif result == 'skipped':
                                    stats['skipped'] += 1
                                else:
                                    stats['errors'] += 1
                
                # Handle individual station record
                elif 'id_station' in data and 'dh_utc' in data:
                    result = self._insert_observation(data, source_name)
                    if result == 'inserted':
                        stats['observations_inserted'] += 1
                    elif result == 'updated':
                        stats['observations_updated'] += 1
                    elif result == 'skipped':
                        stats['skipped'] += 1
                    else:
                        stats['errors'] += 1
            
            except json.JSONDecodeError as e:
                logger.debug(f'Line {line_num}: Invalid JSON - {e}')
                stats['errors'] += 1
            except Exception as e:
                logger.debug(f'Line {line_num}: Processing error - {e}')
                stats['errors'] += 1
        
        return stats
    
    def _upsert_station(self, station: Dict[str, Any]) -> str:
        """
        Upsert station into collection.
        
        Returns:
            'inserted', 'updated', or 'skipped'
        """
        is_valid, error = self.validator.validate_station(station)
        if not is_valid:
            logger.debug(f'Station validation error: {error}')
            return 'skipped'
        
        station_id = station.get('id_station')
        
        # Skip if already cached
        if station_id in self.stations_cache:
            return 'updated'
        
        try:
            result = self.db[COLLECTION_STATIONS].update_one(
                {'id_station': station_id},
                {'$set': station},
                upsert=True
            )
            
            self.stations_cache.add(station_id)
            return 'inserted' if result.upserted_id else 'updated'
        
        except Exception as e:
            logger.warning(f'Error upserting station {station_id}: {e}')
            return 'error'
    
    def _insert_observation(self, obs: Dict[str, Any], source_name: str) -> str:
        """
        Insert observation record.
        
        Returns:
            'inserted', 'updated', 'skipped', or 'error'
        """
        is_valid, error = self.validator.validate_observation(obs)
        if not is_valid:
            logger.debug(f'Observation validation error: {error}')
            return 'skipped'
        
        # Add metadata
        obs['_source'] = source_name
        obs['_ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
        
        try:
            filter_query = {
                'id_station': obs.get('id_station'),
                'dh_utc': obs.get('dh_utc')
            }
            
            result = self.db[COLLECTION_OBSERVATIONS].update_one(
                filter_query,
                {'$set': obs},
                upsert=True
            )
            
            return 'inserted' if result.upserted_id else 'updated'
        
        except Exception as e:
            logger.warning(f'Error inserting observation: {e}')
            return 'error'
    
    def load_schema_metadata(self, output_metadata: Dict[str, str]) -> int:
        """
        Load schema metadata from output_metadata definition.
        
        Args:
            output_metadata: Dict from sources_config.yaml output_metadata
        
        Returns:
            Number of records inserted/updated
        """
        count = 0
        
        for field_name, description in output_metadata.items():
            try:
                # Parse description to extract type and unit info
                schema_doc = {
                    'field_name': field_name,
                    'description': description,
                    'bsonType': self._infer_bson_type(field_name, description),
                    'required': field_name in ['id_station', 'dh_utc'],
                    'indexed': field_name in ['id_station', 'dh_utc', 'city'],
                    'added_at': datetime.now(timezone.utc).isoformat()
                }
                
                self.db[COLLECTION_SCHEMA_METADATA].update_one(
                    {'field_name': field_name},
                    {'$set': schema_doc},
                    upsert=True
                )
                count += 1
            
            except Exception as e:
                logger.warning(f'Error loading schema for {field_name}: {e}')
        
        return count
    
    @staticmethod
    def _infer_bson_type(field_name: str, description: str) -> str:
        """Infer BSON type from field name and description."""
        desc_lower = description.lower()
        
        if 'string' in desc_lower or 'text' in desc_lower:
            return 'string'
        elif 'int' in desc_lower or 'code' in desc_lower:
            return 'int'
        elif 'float' in desc_lower or 'double' in desc_lower or 'decimal' in desc_lower:
            return 'double'
        elif 'bool' in desc_lower:
            return 'bool'
        elif 'date' in desc_lower or 'timestamp' in desc_lower:
            return 'string'  # ISO 8601 as string
        else:
            return 'double'  # Default for numeric measurements


# ============================================================================
# REPORTING (INGESTION ONLY)
# ============================================================================

class IngestionReportGenerator:
    """Generate ingestion loading reports."""
    
    @staticmethod
    def generate_report(load_stats: Dict) -> str:
        """Generate loading summary report."""
        lines = []
        lines.append('=' * 70)
        lines.append('MONGODB INGESTION REPORT - LOADING PHASE')
        lines.append('=' * 70)
        lines.append('')
        
        # Loading summary
        lines.append('LOADING SUMMARY')
        lines.append('-' * 70)
        lines.append(f'Stations - Inserted: {load_stats.get("stations_inserted", 0)}, Updated: {load_stats.get("stations_updated", 0)}')
        lines.append(f'Observations - Inserted: {load_stats.get("observations_inserted", 0)}, Updated: {load_stats.get("observations_updated", 0)}')
        lines.append(f'Schema fields loaded: {load_stats.get("schema_records_inserted", 0)}')
        lines.append(f'Skipped: {load_stats.get("skipped", 0)}, Errors: {load_stats.get("errors", 0)}')
        lines.append('')
        lines.append('=' * 70)
        
        return '\n'.join(lines)
    
    @staticmethod
    def save_report(report: str, output_path: str) -> None:
        """Save report to file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f'✓ Report saved to: {output_path}')


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Load JSONL data into MongoDB (normalized schema)')
    parser.add_argument('--mongodb-uri', default=os.getenv('MONGODB_URI', 'mongodb://localhost:27017'),
                       help='MongoDB connection URI')
    parser.add_argument('--database', default=os.getenv('DATABASE_NAME', 'greencoop_forecast'),
                       help='Target database name')
    parser.add_argument('--local-storage', default=os.getenv('LOCAL_STORAGE', None),
                       help='Local path containing JSONL files')
    parser.add_argument('--s3-bucket', default=os.getenv('S3_BUCKET', None),
                       help='S3 bucket name (if reading from S3)')
    parser.add_argument('--config-file', default=os.getenv('CONFIG_FILE', 'sources_config.yaml'),
                       help='Path to sources_config.yaml for output_metadata')
    parser.add_argument('--drop-collections', action='store_true',
                       help='Drop existing collections before loading')
    parser.add_argument('--report-file', default='logs/ingestion_report.txt',
                       help='Path to save ingestion report')
    
    args = parser.parse_args()
    
    Path('logs').mkdir(exist_ok=True)
    
    logger.info('=' * 70)
    logger.info('MONGODB DATA INGESTION PIPELINE - LOADING PHASE')
    logger.info('=' * 70)
    logger.info('')
    
    try:
        # 1. Connect
        logger.info('STEP 1: Connecting to MongoDB')
        logger.info('-' * 70)
        connection = MongoDBConnection(args.mongodb_uri, args.database)
        if not connection.connect():
            sys.exit(1)
        db = connection.get_database()
        logger.info('')
        
        # 2. Create schema
        logger.info('STEP 2: Creating collections and indexes')
        logger.info('-' * 70)
        schema_mgr = SchemaManager(db)
        if not schema_mgr.create_collections(drop_existing=args.drop_collections):
            sys.exit(1)
        logger.info('')
        
        # 3. Load schema metadata from config
        logger.info('STEP 3: Loading schema metadata')
        logger.info('-' * 70)
        output_metadata = {}
        try:
            import yaml
            config_path = f'config/{args.config_file}'
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                output_metadata = config.get('output_metadata', {})
                logger.info(f'Loaded {len(output_metadata)} fields from output_metadata')
        except Exception as e:
            logger.warning(f'Could not load schema metadata: {e}')
        
        loader = DataLoader(db, DataValidator())
        schema_count = loader.load_schema_metadata(output_metadata)
        logger.info(f'✓ Schema metadata loaded: {schema_count} fields')
        logger.info('')
        
        # 4. Load data
        logger.info('STEP 4: Loading observations and stations')
        logger.info('-' * 70)
        data_source = get_data_source(args.local_storage, args.s3_bucket)
        
        jsonl_files = data_source.list_files(f'{args.local_storage}/*.jsonl' if not args.s3_bucket else f'{args.local_storage}*.jsonl')
        
        if not jsonl_files:
            logger.warning(f'⚠️  No JSONL files found at {args.local_storage}')
        
        total_stats = {
            'stations_inserted': 0,
            'stations_updated': 0,
            'observations_inserted': 0,
            'observations_updated': 0,
            'schema_records_inserted': schema_count,
            'skipped': 0,
            'errors': 0
        }
        
        for jsonl_file in sorted(jsonl_files):
            logger.info(f'Loading: {jsonl_file}')
            try:
                content = data_source.read_file(jsonl_file)
                source_name = Path(jsonl_file).stem
                
                stats = loader.load_jsonl_data(content, source_name)
                
                for key in total_stats:
                    total_stats[key] += stats.get(key, 0)
                
                logger.info(f'  ✓ Stations: {stats.get("stations_inserted", 0)} inserted, {stats.get("stations_updated", 0)} updated')
                logger.info(f'  ✓ Observations: {stats.get("observations_inserted", 0)} inserted, {stats.get("observations_updated", 0)} updated')
                logger.info(f'  ⚠️  Skipped: {stats.get("skipped", 0)}, Errors: {stats.get("errors", 0)}')
            
            except Exception as e:
                logger.error(f'Failed to load {jsonl_file}: {e}')
        
        logger.info('')
        
        # 5. Report
        logger.info('STEP 5: Generating ingestion report')
        logger.info('-' * 70)
        report_gen = IngestionReportGenerator()
        report = report_gen.generate_report(total_stats)
        report_gen.save_report(report, args.report_file)
        logger.info(report)
        
        # 6. Disconnect
        connection.disconnect()
        
        logger.info('')
        logger.info('✅ SUCCESS - Ingestion completed')
        sys.exit(0)
    
    except Exception as e:
        logger.error(f'✗ FAILED: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
