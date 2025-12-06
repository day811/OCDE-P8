# src/quality_checker/quality_checker.py

"""
MongoDB Data Quality Checker
Runs comprehensive quality checks on ingested observations.
Generates detailed quality report and alerts if thresholds violated.

Runs AFTER ingestion to validate MongoDB data.
"""

import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import argparse

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

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
        logging.FileHandler('logs/quality_checker.log', mode='a', encoding='utf-8')
    )

logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

COLLECTION_OBSERVATIONS = 'observations'
COLLECTION_STATIONS = 'stations'
COLLECTION_SCHEMA_METADATA = 'schema_metadata'

# Quality check thresholds (configurable)
THRESHOLDS = {
    'max_null_percentage': 20.0,  # Alert if >20% nulls
    'max_duplicate_percentage': 0.5,  # Alert if >0.5% duplicates
    'temperature_min': -50.0,      # Sanity check
    'temperature_max': 60.0,
    'pressure_min': 900.0,
    'pressure_max': 1050.0
}


# ============================================================================
# MONGODB CONNECTION
# ============================================================================

class MongoDBConnection:
    """Manage MongoDB connection for quality checks."""
    
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
# DATA QUALITY CHECKER
# ============================================================================

class DataQualityChecker:
    """Comprehensive data quality checks on MongoDB observations."""
    
    def __init__(self, db):
        self.db = db
        self.alerts: List[str] = []
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all 7 quality checks."""
        logger.info('Running 7 quality checks...')
        
        obs_count = self.db[COLLECTION_OBSERVATIONS].count_documents({})
        station_count = self.db[COLLECTION_STATIONS].count_documents({})
        schema_count = self.db[COLLECTION_SCHEMA_METADATA].count_documents({})
        
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'collections': {
                'observations_total': obs_count,
                'stations_total': station_count,
                'schema_metadata_total': schema_count
            },
            'checks': {},
            'alerts': []
        }
        
        if obs_count == 0:
            logger.warning('⚠️  No observations in collection')
            return report
        
        # Check 1: Missing required fields
        report['checks']['missing_required_fields'] = self._check_missing_fields(obs_count)
        
        # Check 2: Duplicates
        report['checks']['duplicates'] = self._check_duplicates(obs_count)
        
        # Check 3: Data ranges (temperature, pressure, humidity)
        report['checks']['data_ranges'] = self._check_data_ranges()
        
        # Check 4: Null percentages per measurement field
        report['checks']['null_percentages'] = self._check_null_percentages(obs_count)
        
        # Check 5: Unique values
        report['checks']['unique_values'] = self._check_unique_values()
        
        # Check 6: Type consistency
        report['checks']['type_consistency'] = self._check_type_consistency()
        
        # Check 7: Date range coverage
        report['checks']['date_coverage'] = self._check_date_coverage()
        
        # Add alerts
        report['alerts'] = self.alerts
        
        return report
    
    def _check_missing_fields(self, total_count: int) -> Dict[str, Any]:
        """Check 1: Missing required fields."""
        logger.info('  Check 1: Missing required fields')
        
        missing_station = self.db[COLLECTION_OBSERVATIONS].count_documents({'id_station': None})
        missing_timestamp = self.db[COLLECTION_OBSERVATIONS].count_documents({'dh_utc': None})
        
        result = {
            'id_station': {
                'count': missing_station,
                'percentage': round(missing_station / total_count * 100, 2)
            },
            'dh_utc': {
                'count': missing_timestamp,
                'percentage': round(missing_timestamp / total_count * 100, 2)
            }
        }
        
        if missing_station > 0 or missing_timestamp > 0:
            self.alerts.append(f'⚠️  Missing required fields: {missing_station} station, {missing_timestamp} timestamp')
        
        return result
    
    def _check_duplicates(self, total_count: int) -> Dict[str, Any]:
        """Check 2: Duplicate records."""
        logger.info('  Check 2: Duplicate records')
        
        duplicates = list(self.db[COLLECTION_OBSERVATIONS].aggregate([
            {'$group': {
                '_id': {'station': '$id_station', 'time': '$dh_utc'},
                'count': {'$sum': 1}
            }},
            {'$match': {'count': {'$gt': 1}}}
        ]))
        
        dup_count = len(duplicates)
        dup_percentage = round(dup_count / total_count * 100, 2)
        
        result = {
            'count': dup_count,
            'percentage': dup_percentage
        }
        
        if dup_percentage > THRESHOLDS['max_duplicate_percentage']:
            self.alerts.append(f'⚠️  High duplicate rate: {dup_percentage}%')
        
        return result
    
    def _check_data_ranges(self) -> Dict[str, Any]:
        """Check 3: Data ranges for key measurements."""
        logger.info('  Check 3: Data ranges')
        
        ranges = {}
        measurement_fields = ['temperature', 'pression', 'humidite']
        
        for field in measurement_fields:
            pipeline = [
                {'$match': {field: {'$ne': None, '$type': ['int', 'double']}}},
                {'$group': {
                    '_id': None,
                    'min': {'$min': f'${field}'},
                    'max': {'$max': f'${field}'},
                    'avg': {'$avg': f'${field}'},
                    'count': {'$sum': 1}
                }}
            ]
            
            result = list(self.db[COLLECTION_OBSERVATIONS].aggregate(pipeline))
            
            if result:
                data = result[0]
                ranges[field] = {
                    'min': round(data['min'], 2),
                    'max': round(data['max'], 2),
                    'avg': round(data['avg'], 2),
                    'count': data['count']
                }
                
                # Sanity checks
                if field == 'temperature':
                    if data['min'] < THRESHOLDS['temperature_min']:
                        self.alerts.append(f'⚠️  Temperature below {THRESHOLDS["temperature_min"]}°C: {data["min"]}°C')
                    if data['max'] > THRESHOLDS['temperature_max']:
                        self.alerts.append(f'⚠️  Temperature above {THRESHOLDS["temperature_max"]}°C: {data["max"]}°C')
                
                elif field == 'pression':
                    if data['min'] < THRESHOLDS['pressure_min']:
                        self.alerts.append(f'⚠️  Pressure below {THRESHOLDS["pressure_min"]} hPa: {data["min"]} hPa')
                    if data['max'] > THRESHOLDS['pressure_max']:
                        self.alerts.append(f'⚠️  Pressure above {THRESHOLDS["pressure_max"]} hPa: {data["max"]} hPa')
        
        return ranges
    
    def _check_null_percentages(self, total_count: int) -> Dict[str, Any]:
        """Check 4: Null percentage per measurement field."""
        logger.info('  Check 4: Null percentages')
        
        null_percentages = {}
        measurement_fields = [
            'temperature', 'pression', 'humidite', 'vent_moyen', 'vent_rafales',
            'pluie_1h', 'visibilite', 'nebulosite'
        ]
        
        for field in measurement_fields:
            null_count = self.db[COLLECTION_OBSERVATIONS].count_documents({field: None})
            null_percentage = round(null_count / total_count * 100, 2)
            
            null_percentages[field] = {
                'count': null_count,
                'percentage': null_percentage
            }
            
            if null_percentage > THRESHOLDS['max_null_percentage']:
                self.alerts.append(f'⚠️  High null rate for {field}: {null_percentage}%')
        
        return null_percentages
    
    def _check_unique_values(self) -> Dict[str, int]:
        """Check 5: Unique values."""
        logger.info('  Check 5: Unique values')
        
        result = {
            'unique_stations': len(self.db[COLLECTION_OBSERVATIONS].distinct('id_station')),
            'unique_sources': len(self.db[COLLECTION_OBSERVATIONS].distinct('_source')),
            'unique_cities': self.db[COLLECTION_STATIONS].count_documents({}),
            'schema_fields': self.db[COLLECTION_SCHEMA_METADATA].count_documents({})
        }
        
        return result
    
    def _check_type_consistency(self) -> Dict[str, Any]:
        """Check 6: Type consistency."""
        logger.info('  Check 6: Type consistency')
        
        result = {}
        
        # Check temperature is always numeric or null
        temp_pipeline = [
            {'$match': {'temperature': {'$exists': True}}},
            {'$group': {
                '_id': {'$type': '$temperature'},
                'count': {'$sum': 1}
            }}
        ]
        
        temp_types = list(self.db[COLLECTION_OBSERVATIONS].aggregate(temp_pipeline))
        result['temperature_types'] = {str(t['_id']): t['count'] for t in temp_types}
        
        # Check id_station is always string
        station_pipeline = [
            {'$match': {'id_station': {'$exists': True}}},
            {'$group': {
                '_id': {'$type': '$id_station'},
                'count': {'$sum': 1}
            }}
        ]
        
        station_types = list(self.db[COLLECTION_OBSERVATIONS].aggregate(station_pipeline))
        result['station_types'] = {str(t['_id']): t['count'] for t in station_types}
        
        # Alert if non-string stations found
        if len(result['station_types']) > 1 or (len(result['station_types']) == 1 and 'string' not in list(result['station_types'].keys())[0]):
            self.alerts.append('⚠️  Non-string id_station values detected')
        
        return result
    
    def _check_date_coverage(self) -> Dict[str, Any]:
        """
        Check date coverage - ensure continuous date range without gaps.
        """
        logger.info('  Check 7: Date coverage')

        try:
            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'min_date': { '$min': '$dh_utc' },
                        'max_date': { '$max': '$dh_utc' },
                        'unique_dates': { '$addToSet': '$dh_utc' }
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'min_date': 1,
                        'max_date': 1,
                        'date_count': { '$size': '$unique_dates' }
                    }
                }
            ]
            
            result = list(self.db[COLLECTION_OBSERVATIONS].aggregate(pipeline))
            
            if not result or not result[0].get('min_date'):
                return {
                    'status': 'WARN',
                    'reason': 'No date data or aggregation failed',
                    'details': {
                        'min_date': 'N/A',
                        'max_date': 'N/A',
                        'coverage_percentage': 0
                    }
                }
            
            stats = result[0]
            
            from dateutil import parser as date_parser
            
            try:
                min_dt = date_parser.parse(stats['min_date'])
                max_dt = date_parser.parse(stats['max_date'])
                
                total_days = (max_dt - min_dt).days + 1
                actual_days = stats.get('date_count', 0)
                coverage_ratio = (actual_days / total_days * 100) if total_days > 0 else 0
                
                return {
                    'status': 'PASS' if coverage_ratio >= 95 else 'WARN',
                    'reason': f'Date coverage: {coverage_ratio:.1f}%',
                    'details': {
                        'min_date': stats['min_date'],
                        'max_date': stats['max_date'],
                        'total_days': total_days,
                        'unique_dates': actual_days,
                        'coverage_percentage': round(coverage_ratio, 2)
                    }
                }
            except Exception as parse_error:
                logger.warning(f'Date parsing error: {parse_error}')
                return {
                    'status': 'WARN',
                    'reason': f'Date parsing failed: {str(parse_error)}',
                    'details': {
                        'min_date': str(stats.get('min_date')),
                        'max_date': str(stats.get('max_date')),
                        'coverage_percentage': 0
                    }
                }
        
        except Exception as e:
            logger.warning(f'Date coverage check error: {str(e)}')
            return {
                'status': 'ERROR',
                'reason': f'Aggregation failed: {str(e)}',
                'details': {}
            }

# ============================================================================
# REPORTING
# ============================================================================

class QualityReportGenerator:
    """Generate comprehensive quality reports."""
    
    @staticmethod
    def generate_report(quality_report: Dict) -> str:
        """Generate formatted quality report."""
        lines = []
        lines.append('=' * 80)
        lines.append('MONGODB DATA QUALITY REPORT - POST-INGESTION ANALYSIS')
        lines.append('=' * 80)
        lines.append('')
        
        # Timestamp and collection stats
        lines.append(f'Report generated: {quality_report.get("timestamp", "N/A")}')
        lines.append('')
        
        lines.append('COLLECTION STATISTICS')
        lines.append('-' * 80)
        collections = quality_report.get('collections', {})
        lines.append(f'Total observations: {collections.get("observations_total", 0)}')
        lines.append(f'Total stations: {collections.get("stations_total", 0)}')
        lines.append(f'Schema fields defined: {collections.get("schema_metadata_total", 0)}')
        lines.append('')
        
        # Quality checks
        lines.append('QUALITY CHECKS')
        lines.append('-' * 80)
        checks = quality_report.get('checks', {})
        
        # Check 1: Missing fields
        if 'missing_required_fields' in checks:
            lines.append('✓ Check 1: Missing Required Fields')
            for field, data in checks['missing_required_fields'].items():
                lines.append(f'    {field}: {data["count"]} records ({data["percentage"]}%)')
        
        # Check 2: Duplicates
        if 'duplicates' in checks:
            dup = checks['duplicates']
            lines.append(f'✓ Check 2: Duplicates: {dup["count"]} records ({dup["percentage"]}%)')
        
        # Check 3: Data ranges
        if 'data_ranges' in checks:
            lines.append('✓ Check 3: Data Ranges')
            for field, data in checks['data_ranges'].items():
                lines.append(f'    {field}:')
                lines.append(f'      Min: {data["min"]}, Max: {data["max"]}, Avg: {data["avg"]}')
                lines.append(f'      Records with data: {data["count"]}')
        
        # Check 4: Null percentages
        if 'null_percentages' in checks:
            lines.append('✓ Check 4: Null Percentages')
            for field, data in checks['null_percentages'].items():
                lines.append(f'    {field}: {data["percentage"]}%')
        
        # Check 5: Unique values
        if 'unique_values' in checks:
            lines.append('✓ Check 5: Unique Values')
            for key, count in checks['unique_values'].items():
                lines.append(f'    {key}: {count}')
        
        # Check 6: Type consistency
        if 'type_consistency' in checks:
            lines.append('✓ Check 6: Type Consistency')
            for key, types in checks['type_consistency'].items():
                lines.append(f'    {key}: {types}')
        
        # Check 7: Date coverage
        if 'date_coverage' in checks:
            lines.append('✓ Check 7: Date Coverage')
            coverage = checks['date_coverage']
            details = coverage.get('details', {})  # ← RÉCUPÉRER les détails
            if 'error' not in coverage:
                lines.append(f' Min date: {details.get("min_date", "N/A")}')  # ← Utiliser details
                lines.append(f' Max date: {details.get("max_date", "N/A")}')  # ← Utiliser details
                lines.append(f' Coverage: {details.get("coverage_percentage", 0)}%') 
                
                lines.append('')
        
        # Alerts
        alerts = quality_report.get('alerts', [])
        if alerts:
            lines.append('ALERTS')
            lines.append('-' * 80)
            for alert in alerts:
                lines.append(alert)
            lines.append('')
        else:
            lines.append('✅ No alerts - Data quality is excellent')
            lines.append('')
        
        lines.append('=' * 80)
        
        return '\n'.join(lines)
    
    @staticmethod
    def save_report(report: str, output_path: str) -> None:
        """Save report to file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f'✓ Quality report saved to: {output_path}')


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run data quality checks on MongoDB observations')
    parser.add_argument('--config-file', default=os.getenv('CONFIG_FILE', 'sources_config.yaml'),
                       help='Path to sources_config.yaml for output_metadata')
    parser.add_argument('--mongodb-uri', default=os.getenv('MONGODB_URI', 'mongodb://localhost:27017'),
                       help='MongoDB connection URI')
    parser.add_argument('--database', default=os.getenv('DATABASE_NAME', 'greencoop_forecast'),
                       help='Target database name')
    parser.add_argument('--report-file', default='logs/quality_report.txt',
                       help='Path to save quality report')
    
    args = parser.parse_args()
    
    if os.getenv('DOCKMODE') or os.getenv('SUBNET_ID'):
        args.mongodb_uri =  str(args.mongodb_uri).replace('@localhost:', '@mongodb:')
    else:
        args.mongodb_uri =  str(args.mongodb_uri).replace('@mongodb:', '@localhost:')
    
    Path('logs').mkdir(exist_ok=True)
    
    logger.info('=' * 80)
    logger.info('MONGODB DATA QUALITY CHECKER - POST-INGESTION ANALYSIS')
    logger.info('=' * 80)
    logger.info('')
    
    try:
        # 1. Connect
        logger.info('STEP 1: Connecting to MongoDB')
        logger.info('-' * 80)
        connection = MongoDBConnection(args.mongodb_uri, args.database)
        if not connection.connect():
            sys.exit(1)
        db = connection.get_database()
        logger.info('')
        
        # 2. Run checks
        logger.info('STEP 2: Running 7 quality checks')
        logger.info('-' * 80)
        quality_checker = DataQualityChecker(db)
        quality_report = quality_checker.run_all_checks()
        logger.info('')
        
        # 3. Generate report
        logger.info('STEP 3: Generating quality report')
        logger.info('-' * 80)
        report_gen = QualityReportGenerator()
        report = report_gen.generate_report(quality_report)
        report_gen.save_report(report, args.report_file)
        logger.info(report)
        
        # 4. Disconnect
        connection.disconnect()
        
        logger.info('')
        logger.info('✅ SUCCESS - Quality check completed')
        sys.exit(0)
    
    except Exception as e:
        logger.error(f'✗ FAILED: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
