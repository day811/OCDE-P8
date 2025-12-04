# src/preprocessing/unified_data_pipeline.py

import boto3
import json
import yaml
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any
from abc import ABC, abstractmethod
import logging
import sys
import os
import argparse
import requests


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

# Détecter si on est en ECS
IS_ECS = os.getenv('SUBNET_ID') is not None

handlers = [
    logging.StreamHandler(sys.stdout),  # Toujours vers stdout
]

# En local/dev, ajouter fichier
if not IS_ECS:
    handlers.append(
        logging.FileHandler('logs/preprocessing.log', mode='a', encoding='utf-8')
    )

logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)

logger = logging.getLogger(__name__)


# ============================================================================
# STRUCTURE INFOCLIMAT (source de vérité)
# ============================================================================

"""
Expected structure for all outputs:
{
  "status": "OK",
  "data": {
    "stations": [
      {
        "id": "ILAMAD25",
        "name": "La Madeleine",
        "latitude": 50.659,
        "longitude": 3.07,
        "elevation": 23,
        "city": "La Madeleine",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6"
      },
      ...
    ]
  },
  "hourly": {
    "ILAMAD25": [
      {
        "id_station": "ILAMAD25",
        "dh_utc": "2024-10-01T00:00:00Z",
        "temperature": 15.0,
        "pression": 1013.0,
        ...
      },
      ...
    ],
    "00052": [ ... ]
  }
}
"""

# ============================================================================
# BASE CLASS
# ============================================================================

class DataSourceHandler(ABC):
    """Base class for handling different data source types."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_name = config.get('source_name')
        self.skip_empty_rows = config.get('skip_empty_rows', True)
        self.skip_header_blank_rows = config.get('skip_header_blank_rows', True)
    
    def _download_file(self, url: str, output_path: str = None) -> str:
        """Download file from URL."""
        try:
            logger.info(f"    Downloading from: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            if output_path is None:
                filename = url.split('/')[-1]
                output_path = f"/tmp/{filename}"
            
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"    ✓ Downloaded to: {output_path}")
            return output_path
        
        except Exception as e:
            logger.error(f"    ✗ Download failed: {e}")
            raise
    
    def filter_empty_rows(self, data: List[Dict]) -> List[Dict]:
        """Remove rows where all measurement values are null/empty."""
        filtered = []
        measurement_fields = [
            'temperature', 'pression', 'humidite', 'vent_moyen', 
            'vent_rafales', 'pluie_3h', 'pluie_1h'
        ]
        
        for row in data:
            has_measurement = any(
                row.get(field) is not None and str(row.get(field, '')).strip() 
                for field in measurement_fields
            )
            if has_measurement:
                filtered.append(row)
        
        return filtered
    
    @abstractmethod
    def read(self) -> tuple:
        """Return (stations_list, hourly_records_dict)."""
        pass
    
    @abstractmethod
    def normalize(self) -> tuple:
        """Return (stations_list, hourly_records_dict) normalized."""
        pass
    
    def process(self) -> tuple:
        """Full processing pipeline."""
        logger.info(f"Processing source: {self.source_name}")
        
        stations, hourly_records = self.read()
        logger.info(f"  ✓ Read {len(stations)} stations")
        logger.info(f"  ✓ Read {sum(len(r) for r in hourly_records.values())} records total")
        
        if self.skip_empty_rows:
            # Filter empty records per station
            for station_id in hourly_records:
                hourly_records[station_id] = self.filter_empty_rows(hourly_records[station_id])
            logger.info(f"  ✓ After filtering: {sum(len(r) for r in hourly_records.values())} records")
        
        return stations, hourly_records

# ============================================================================
# EXCEL HANDLER (Wunderground)
# ============================================================================

class ExcelDataHandler(DataSourceHandler):
    """Handler for Excel files from Weather Underground."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.file_path = config.get('file_path')
        self.url = config.get('url')
        
        # Handle URL in file_path
        if self.file_path and str(self.file_path).startswith(('http://', 'https://')):
            self.url = self.file_path
            self.file_path = None
        
        if self.url and not self.file_path:
            self.file_path = self._download_file(self.url)
        
        # Station metadata
        self.station_id = config.get('station_id')
        self.station_name = config.get('station_name')
        self.latitude = config.get('latitude')
        self.longitude = config.get('longitude')
        self.elevation = config.get('elevation')
        self.city = config.get('city')
        self.state = config.get('state')
        self.hardware = config.get('hardware')
        self.software = config.get('software')
    
    def parse_sheet_date(self, sheet_name: str) -> str:
        """Parse DDMMYY format to date."""
        try:
            return datetime.strptime(sheet_name.strip(), "%d%m%y").strftime("%Y-%m-%d")
        except:
            return sheet_name
    
    def read(self) -> tuple:
        """Read Excel and return (stations, hourly_records)."""
        # Station definition (static for this source)
        stations = [{
            'id': self.station_id,
            'name': self.station_name,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'elevation': self.elevation,
            'city': self.city,
            'state': self.state,
            'hardware': self.hardware,
            'software': self.software
        }]
        
        # Read Excel sheets
        hourly_records = {self.station_id: []}
        
        xls = pd.ExcelFile(self.file_path)
        logger.info(f"    Found {len(xls.sheet_names)} sheets")
        
        for sheet_name in xls.sheet_names:
            logger.info(f"    Reading sheet: {sheet_name}")
            
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, dtype=str)
            
            if self.skip_header_blank_rows:
                df = df.dropna(how='all')
            
            logger.info(f"      → {len(df)} rows")
            
            sheet_date = self.parse_sheet_date(sheet_name)
            
            # Convert to dict records
            records = df.to_dict('records')
            
            for row in records:
                try:
                    time_str = row.get('Time', '00:00:00')
                    dh_utc = f"{sheet_date}T{time_str}Z" if sheet_date else None
                    
                    normalized_row = {
                        'id_station': self.station_id,
                        'dh_utc': dh_utc,
                        'temperature': self._fahrenheit_to_celsius(row.get('Temperature')),
                        'pression': self._extract_pressure(row.get('Pressure')),
                        'humidite': self._extract_percentage(row.get('Humidity')),
                        'point_de_rosee': self._fahrenheit_to_celsius(row.get('Dew Point')),
                        'visibilite': None,
                        'vent_moyen': self._mph_to_kmh(row.get('Speed')),
                        'vent_rafales': self._mph_to_kmh(row.get('Gust')),
                        'vent_direction': self._extract_wind_direction_degrees(row.get('Wind')),
                        'pluie_3h': None,
                        'pluie_1h': self._extract_precipitation(row.get('Precip. Accum.')),
                        'neige_au_sol': None,
                        'nebulosite': None,
                        'temps_omm': None
                    }
                    
                    hourly_records[self.station_id].append(normalized_row)
                
                except Exception as e:
                    logger.warning(f"      ⚠️  Row error: {e}")
                    continue
        
        return stations, hourly_records
    
    def normalize(self) -> tuple:
        """Already normalized in read()."""
        return self.read()
    
    # Utility methods
    def _fahrenheit_to_celsius(self, temp_str):
        if not temp_str or str(temp_str).strip() == '' or str(temp_str).upper() == 'NAN':
            return None
        try:
            f_val = float(str(temp_str).replace('°F', '').strip())
            return round((f_val - 32) * 5 / 9, 1)
        except:
            return None
    
    def _mph_to_kmh(self, speed_str):
        if not speed_str or str(speed_str).upper() == 'NAN':
            return None
        try:
            mph = float(str(speed_str).replace('mph', '').strip())
            return round(mph * 1.60934, 1)
        except:
            return None
    
    def _extract_percentage(self, value_str):
        if not value_str or str(value_str).upper() == 'NAN':
            return None
        try:
            return float(str(value_str).replace('%', '').strip())
        except:
            return None
    
    def _extract_pressure(self, pressure_str):
        if not pressure_str or str(pressure_str).upper() == 'NAN':
            return None
        try:
            in_val = float(str(pressure_str).replace('in', '').strip())
            return round(in_val * 33.8639, 1)
        except:
            return None
    
    def _extract_precipitation(self, precip_str):
        if not precip_str or str(precip_str).upper() == 'NAN':
            return None
        try:
            in_val = float(str(precip_str).replace('in', '').strip())
            return round(in_val * 25.4, 1)
        except:
            return None
    
    def _extract_wind_direction_degrees(self, direction_str):
        if not direction_str:
            return None
        
        direction_map = {
            'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90,
            'ESE': 112.5, 'SE': 135, 'SSE': 157.5, 'S': 180,
            'SSW': 202.5, 'SW': 225, 'WSW': 247.5, 'W': 270,
            'WNW': 292.5, 'NW': 315, 'NNW': 337.5
        }
        
        direction_upper = str(direction_str).strip().upper()
        return direction_map.get(direction_upper, None)

# ============================================================================
# JSON HANDLER (InfoClimat)
# ============================================================================

class JSONDataHandler(DataSourceHandler):
    """Handler for InfoClimat JSON API response."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.file_path = config.get('file_path')
        self.url = config.get('url')
        
        # Handle URL in file_path
        if self.file_path and str(self.file_path).startswith(('http://', 'https://')):
            self.url = self.file_path
            self.file_path = None
        
        if self.url and not self.file_path:
            self.file_path = self._download_file(self.url)
    
    def read(self) -> tuple:
        """Read InfoClimat JSON and respect structure."""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract stations
        stations_raw = data.get('stations', [])
        stations = []
        
        for station in stations_raw:
            stations.append({
                'id': station.get('id'),
                'name': station.get('name'),
                'latitude': station.get('latitude'),
                'longitude': station.get('longitude'),
                'elevation': station.get('elevation'),
                'city': station.get('name'),
                'state': None,
                'hardware': None,
                'software': None
            })
        
        logger.info(f"    Extracted {len(stations)} stations from metadata")
        
        # Extract hourly records (by station)
        hourly_raw = data.get('hourly', {})
        hourly_records = {}
        
        for station_id, records in hourly_raw.items():
            if isinstance(records, list) and station_id != "_params":
                logger.info(f"    Station {station_id}: {len(records)} records")
                
                normalized = []
                for row in records:
                    try:
                        normalized_row = {
                            'id_station': row.get('id_station'),
                            'dh_utc': self._normalize_timestamp(row.get('dh_utc')),
                            'temperature': self._to_float(row.get('temperature')),
                            'pression': self._to_float(row.get('pression')),
                            'humidite': self._to_float(row.get('humidite')),
                            'point_de_rosee': self._to_float(row.get('point_de_rosee')),
                            'visibilite': self._to_float(row.get('visibilite')),
                            'vent_moyen': self._to_float(row.get('vent_moyen')),
                            'vent_rafales': self._to_float(row.get('vent_rafales')),
                            'vent_direction': self._to_float(row.get('vent_direction')),
                            'pluie_3h': self._to_float(row.get('pluie_3h')),
                            'pluie_1h': self._to_float(row.get('pluie_1h')),
                            'neige_au_sol': self._to_float(row.get('neige_au_sol')),
                            'nebulosite': row.get('nebulosite'),
                            'temps_omm': self._to_int(row.get('temps_omm'))
                        }
                        normalized.append(normalized_row)
                    except Exception as e:
                        logger.warning(f"      ⚠️  Record error: {e}")
                        continue
                
                hourly_records[station_id] = normalized
        
        return stations, hourly_records
    
    def normalize(self) -> tuple:
        """Already normalized in read()."""
        return self.read()
        
    def _normalize_timestamp(self, ts_str: str) -> str:
        """
        Convert timestamp to ISO 8601 format.
         
        Returns ISO 8601 string or None
        """
        if not ts_str or not str(ts_str).strip():
            return None
        
        try:
            # Parser accepte plusieurs formats
            from dateutil import parser
            dt = parser.parse(str(ts_str))
            # ✅ Retourner ISO 8601 strict
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError):
            logger.warning(f'Invalid timestamp format: {ts_str}')
            return None
        

    def _to_float(self, value):
        if value is None or value == '' or str(value).lower() in ['null', 'nan']:
            return None
        try:
            return float(value)
        except:
            return None
    
    def _to_int(self, value):
        if value is None or value == '' or str(value).lower() in ['null', 'nan']:
            return None
        try:
            return int(float(value))
        except:
            return None

# ============================================================================
# UNIFIED PIPELINE
# ============================================================================

class UnifiedDataPipeline:
    """Main orchestration."""
    
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
        self.handlers = self._initialize_handlers()
    
    def _load_config(self) -> Dict:
        """Load YAML configuration."""
        with open(self.config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _initialize_handlers(self) -> Dict[str, DataSourceHandler]:
        """Initialize handlers for each source."""
        handlers = {}
        
        for source_id, source_config in self.config['sources'].items():
            source_type = source_config.get('type', 'unknown')
            
            if source_type == 'excel':
                handlers[source_id] = ExcelDataHandler(source_config)
            elif source_type == 'json':
                handlers[source_id] = JSONDataHandler(source_config)
        
        return handlers
    
    def process_all_sources(self) -> Dict[str, tuple]:
        """Process all sources."""
        logger.info("=" * 70)
        logger.info("UNIFIED DATA PIPELINE - Processing All Sources")
        logger.info("=" * 70)
        logger.info("")
        
        results = {}
        
        for source_id, handler in self.handlers.items():
            try:
                stations, hourly_records = handler.process()
                results[source_id] = (stations, hourly_records)
                total_records = sum(len(r) for r in hourly_records.values())
                logger.info(f"✓ {source_id}: {len(stations)} stations, {total_records} records")
                logger.info("")
            except Exception as e:
                logger.error(f"✗ {source_id}: {e}", exc_info=True)
                logger.error("")
        
        return results
    
    def merge_sources(self, results: Dict[str, tuple]) -> tuple:
        """Merge all sources into unified structure."""
        all_stations = []
        all_hourly = {}
        
        for source_id, (stations, hourly_records) in results.items():
            # Merge stations (deduplicate by ID)
            for station in stations:
                if not any(s['id'] == station['id'] for s in all_stations):
                    all_stations.append(station)
            
            # Merge hourly records (keep separate by station)
            for station_id, records in hourly_records.items():
                if station_id not in all_hourly:
                    all_hourly[station_id] = []
                all_hourly[station_id].extend(records)
        
        return all_stations, all_hourly
    
    def save_normalized_data(self, results: Dict[str, tuple], local_storage: str = None):
        """Save to JSONL files locally or to S3."""
        
        logger.info("=" * 70)
        logger.info("Saving Normalized Data to JSONL")
        logger.info("=" * 70)
        logger.info("")
        
        # Merge all sources
        all_stations, all_hourly = self.merge_sources(results)
        
        unified_structure = {
            "status": "OK",
            "stations": all_stations,
            "metadata": self.config['output_metadata'],
            "hourly": all_hourly
        }
        # set filename
        s3_path = os.getenv('S3_PATH', 'data')
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        self.filename = f"{s3_path}_{timestamp}.jsonl"

        
        # Determine save destination
        if local_storage and local_storage.strip():
            # Save locally
            logger.info(f"Saving to local directory: {local_storage}")
            return self._save_local(unified_structure, local_storage, all_stations, all_hourly)
        else:
            # Save to S3
            logger.info("Saving to S3 (no local directory specified)")
            return self._save_s3(unified_structure, all_stations, all_hourly)
        
    
    def _save_local(self, unified_structure: Dict, local_storage: str, all_stations: List, all_hourly: Dict):
        """Save to local filesystem."""
        Path(local_storage).mkdir(parents=True, exist_ok=True)
        
        
        output_file = f"{local_storage}/{self.filename}"

        # Save as single JSONL record (one line = complete structure)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(json.dumps(unified_structure, ensure_ascii=False, default=str) + '\n')
        
        logger.info(f"✓ {len(all_stations)} stations + {sum(len(r) for r in all_hourly.values())} records")
        logger.info(f"  → {output_file}")
        logger.info("")
        
        return output_file
    
    def _save_s3(self, unified_structure: Dict, all_stations: List, all_hourly: Dict):

        """Save to S3 bucket."""
        
        # Get AWS credentials from .env
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION', 'eu-west-3')
        s3_bucket = os.getenv('S3_BUCKET')
        s3_path = os.getenv('S3_PATH', 'data')
        
        # Validate credentials
        if not aws_access_key_id or not aws_secret_access_key:
            logger.error("✗ AWS credentials not found in .env file")
            logger.error("  Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            logger.error("  Falling back to local save in data/clean")
            return self._save_local(unified_structure, 'data/clean', all_stations, all_hourly)
        
        if not s3_bucket:
            logger.error("✗ S3_BUCKET not found in .env file")
            logger.error("  Please set S3_BUCKET")
            logger.error("  Falling back to local save in data/clean")
            return self._save_local(unified_structure, 'data/clean', all_stations, all_hourly)
        
        try:
            # Initialize S3 client with explicit credentials
            s3_client = boto3.client(
                's3',
                region_name=aws_region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            
            # Test connection
            logger.info(f"Testing S3 connection to bucket: {s3_bucket}")
            s3_client.head_bucket(Bucket=s3_bucket)
            logger.info("✓ S3 connection successful")
            
            # Determine S3 path
            s3_key = f"{s3_path}/{self.filename}"
            
            # Prepare content
            content = json.dumps(unified_structure, ensure_ascii=False, default=str) + '\n'
            
            # Upload to S3
            logger.info(f"Uploading to S3: s3://{s3_bucket}/{s3_key}")
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=content.encode('utf-8'),
                ContentType='application/x-ndjson'
            )
            
            logger.info(f"✓ {len(all_stations)} stations + {sum(len(r) for r in all_hourly.values())} records")
            logger.info(f"  → s3://{s3_bucket}/{s3_key}")
            logger.info("")
            
            return f"s3://{s3_bucket}/{s3_key}"
        
        except Exception as e:
            logger.error(f"✗ S3 upload failed: {e}")
            logger.error("  Falling back to local save in data/clean")
            return self._save_local(unified_structure, 'data/clean', all_stations, all_hourly)
            
    def run(self, local_storage: str = None):
        """Execute full pipeline."""
        results = self.process_all_sources()
        saved_file = self.save_normalized_data(results, local_storage)
        
        logger.info("=" * 70)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info("=" * 70)
        
        return results, saved_file

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':

    load_dotenv()

    parser = argparse.ArgumentParser(description='Unified Data Pipeline')
    parser.add_argument(
        '--config-file',
        default=os.getenv('CONFIG_FILE','config.yaml'),
        help='Configuration file name in config/'
    )
    parser.add_argument(
        '--local-storage',
        default= os.getenv('LOCAL_STORAGE',None),
        help='Set Output to local directory or S3(blanck/empty)'
    )
    parser.add_argument(
        '--log-to-stdout',
        default= None,
        help='Redirect log to stdout'
    )
    
    args = parser.parse_args()
    logger.info(f"Configuration file name : {args.config_file}")
    logger.info(f"Local storage : {args.local_storage}")
    logger.info(f"Configuration file path : {os.getenv('S3_PATH','None')}")
    logger.info(f"S3 bucket : {os.getenv('S3_BUCKET','None')}")

    
    try:
        pipeline = UnifiedDataPipeline(args.config_file)
        results, saved_file = pipeline.run(args.local_storage)
        logger.info("✅ SUCCESS")
        sys.exit(0)
    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
