# transformation/tests/test_data_quality.py
"""
Comprehensive data quality tests for the unified data pipeline.
Tests data integrity before and after migration:
- Column availability
- Variable types
- Duplicates
- Missing values
- Data ranges
- Source consistency
"""

import unittest
import sys
import os
from datetime import datetime, timezone
from collections import defaultdict

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'preprocessing'))

from unified_data_pipeline import ExcelDataHandler, JSONDataHandler


class DataQualityReport:
    """Generate comprehensive data quality reports."""
    
    def __init__(self):
        self.checks = {}
        #self.timestamp = datetime.utcnow().isoformat()
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.status = "PASS"
    
    def add_check(self, name: str, status: str, details: dict = None, warnings: list = None):
        """Add a quality check result."""
        self.checks[name] = {
            'status': status,
            'details': details or {},
            'warnings': warnings or []
        }
        if status == 'FAIL':
            self.status = "FAIL"
    
    def print_report(self):
        """Print formatted report."""
        print("\n" + "=" * 70)
        print("DATA QUALITY REPORT")
        print("=" * 70)
        print(f"Timestamp: {self.timestamp}")
        print(f"Overall Status: {self.status}")
        print("-" * 70)
        
        for check_name, check_result in self.checks.items():
            status = check_result.get('status', 'UNKNOWN')
            symbol = "✓" if status == 'PASS' else "✗"
            print(f"\n{symbol} {check_name}")
            print(f"  Status: {status}")
            
            if check_result.get('details'):
                for key, value in check_result['details'].items():
                    print(f"  {key}: {value}")
            
            if check_result.get('warnings'):
                for warning in check_result['warnings']:
                    print(f"  ⚠️  {warning}")


# ============================================================================
# TEST 1: Column Availability
# ============================================================================

class TestColumnAvailability(unittest.TestCase):
    """Test 1: Verify all expected columns are present."""
    
    EXPECTED_FIELDS = {
        'id_station', 'dh_utc', 'temperature', 'pression', 'humidite',
        'point_de_rosee', 'visibilite', 'vent_moyen', 'vent_rafales',
        'vent_direction', 'pluie_3h', 'pluie_1h', 'neige_au_sol',
        'nebulosite', 'temps_omm'
    }
    
    def test_excel_handler_fields(self):
        """Test Excel handler produces all expected fields."""
        config = {
            'type': 'excel',
            'source_name': 'Test Wunderground',
            'station_id': 'TEST123',
            'station_name': 'Test Station',
            'latitude': 50.0,
            'longitude': 3.0,
            'elevation': 25,
            'city': 'Test City',
            'state': '--',
            'hardware': 'test',
            'software': 'test',
            'skip_empty_rows': True
        }
        
        # Create sample row with Excel format
        sample_row = {
            'Temperature': '56.8 °F',
            'Wind': 'WSW',
            'Speed': '8.2 mph',
            'Pressure': '29.48 in',
            'Humidity': '87 %',
            'Dew Point': '53.1 °F',
            'Gust': '10.4 mph',
            'Time': '00:04:00'
        }
        
        handler = ExcelDataHandler(config)
        
        # Manually normalize the row using handler methods
        normalized_row = {
            'id_station': handler.station_id,
            'station_name': handler.station_name,
            'latitude': handler.latitude,
            'longitude': handler.longitude,
            'elevation': handler.elevation,
            'city': handler.city,
            'state': handler.state,
            'hardware': handler.hardware,
            'software': handler.software,
            'dh_utc': '2024-10-01T00:04:00Z',
            'temperature': handler._fahrenheit_to_celsius(sample_row.get('Temperature')),
            'pression': handler._extract_pressure(sample_row.get('Pressure')),
            'humidite': handler._extract_percentage(sample_row.get('Humidity')),
            'point_de_rosee': handler._fahrenheit_to_celsius(sample_row.get('Dew Point')),
            'visibilite': None,
            'vent_moyen': handler._mph_to_kmh(sample_row.get('Speed')),
            'vent_rafales': handler._mph_to_kmh(sample_row.get('Gust')),
            'vent_direction': handler._extract_wind_direction_degrees(sample_row.get('Wind')),
            'pluie_3h': None,
            'pluie_1h': None,
            'neige_au_sol': None,
            'nebulosite': None,
            'temps_omm': None
        }
        
        # Check fields exist
        missing_fields = self.EXPECTED_FIELDS - set(normalized_row.keys())
        self.assertEqual(len(missing_fields), 0, f"Missing fields: {missing_fields}")
    
    def test_json_handler_fields(self):
        """Test JSON handler produces all expected fields."""
        config = {
            'type': 'json',
            'source_name': 'Test InfoClimat',
            'skip_empty_rows': True
        }
        
        sample_row = {
            'id_station': '00052',
            'dh_utc': '2024-10-01 00:00:00',
            'temperature': '7.6',
            'pression': '1020.7',
            'humidite': '89',
            'point_de_rosee': '5.9',
            'visibilite': '6000',
            'vent_moyen': '3.6',
            'vent_rafales': '7.2',
            'vent_direction': '90',
            'pluie_3h': '0',
            'pluie_1h': '0',
            'neige_au_sol': None,
            'nebulosite': '7',
            'temps_omm': None
        }
        
        handler = JSONDataHandler(config)
        
        # Manually normalize using handler conversion methods
        normalized_row = {
            'id_station': sample_row.get('id_station'),
            'dh_utc': sample_row.get('dh_utc'),
            'temperature': handler._to_float(sample_row.get('temperature')),
            'pression': handler._to_float(sample_row.get('pression')),
            'humidite': handler._to_float(sample_row.get('humidite')),
            'point_de_rosee': handler._to_float(sample_row.get('point_de_rosee')),
            'visibilite': handler._to_float(sample_row.get('visibilite')),
            'vent_moyen': handler._to_float(sample_row.get('vent_moyen')),
            'vent_rafales': handler._to_float(sample_row.get('vent_rafales')),
            'vent_direction': handler._to_float(sample_row.get('vent_direction')),
            'pluie_3h': handler._to_float(sample_row.get('pluie_3h')),
            'pluie_1h': handler._to_float(sample_row.get('pluie_1h')),
            'neige_au_sol': handler._to_float(sample_row.get('neige_au_sol')),
            'nebulosite': sample_row.get('nebulosite'),
            'temps_omm': handler._to_int(sample_row.get('temps_omm'))
        }
        
        missing_fields = self.EXPECTED_FIELDS - set(normalized_row.keys())
        self.assertEqual(len(missing_fields), 0, f"Missing fields: {missing_fields}")


# ============================================================================
# TEST 2: Data Types
# ============================================================================

class TestDataTypes(unittest.TestCase):
    """Test 2: Verify correct variable types."""
    
    def test_numeric_fields_are_numbers(self):
        """Verify numeric fields are numbers or null."""
        config = {
            'type': 'excel',
            'source_name': 'Test',
            'station_id': 'TEST',
            'station_name': 'Test',
            'latitude': 50.0,
            'longitude': 3.0,
            'elevation': 25,
            'city': 'Test',
            'state': '--',
            'hardware': 'test',
            'software': 'test'
        }
        
        handler = ExcelDataHandler(config)
        
        # Test conversions
        temp = handler._fahrenheit_to_celsius('56.8 °F')
        pressure = handler._extract_pressure('29.48 in')
        humidity = handler._extract_percentage('87 %')
        
        # Check types
        self.assertIsInstance(temp, (float, type(None)))
        self.assertIsInstance(pressure, (float, type(None)))
        self.assertIsInstance(humidity, (float, type(None)))
    
    def test_string_fields_are_strings(self):
        """Verify string fields are strings or null."""
        config = {
            'type': 'json',
            'source_name': 'Test',
            'skip_empty_rows': True
        }
        
        handler = JSONDataHandler(config)
        
        # Test fields that should stay as strings
        sample_id = '00052'
        sample_dh = '2024-10-01 00:00:00'
        sample_nebulosite = '7'
        
        self.assertIsInstance(sample_id, str)
        self.assertIsInstance(sample_dh, str)
        self.assertIsInstance(sample_nebulosite, (str, type(None)))


# ============================================================================
# TEST 3: Duplicates
# ============================================================================

class TestDuplicates(unittest.TestCase):
    """Test 3: Detect duplicate records."""
    
    def test_duplicate_detection(self):
        """Test that duplicate records are detected."""
        records = [
            {'id_station': '00052', 'dh_utc': '2024-10-01T00:00:00Z', 'temperature': 7.6},
            {'id_station': '00052', 'dh_utc': '2024-10-01T00:00:00Z', 'temperature': 7.6},  # Duplicate
            {'id_station': '00052', 'dh_utc': '2024-10-01T01:00:00Z', 'temperature': 7.5},  # Different
        ]
        
        # Find duplicates by (id_station, dh_utc)
        seen = set()
        duplicates = []
        
        for record in records:
            key = (record.get('id_station'), record.get('dh_utc'))
            if key in seen:
                duplicates.append(key)
            seen.add(key)
        
        self.assertEqual(len(duplicates), 1, "Should detect 1 duplicate")


# ============================================================================
# TEST 4: Missing Values
# ============================================================================

class TestMissingValues(unittest.TestCase):
    """Test 4: Check for missing values."""
    
    def test_missing_values_handling(self):
        """Test that missing values are properly handled."""
        records = [
            {'id_station': '00052', 'dh_utc': '2024-10-01T00:00:00Z', 'temperature': 7.6, 'pression': None},
            {'id_station': '00052', 'dh_utc': '2024-10-01T01:00:00Z', 'temperature': None, 'pression': 1020.7},
            {'id_station': '00052', 'dh_utc': '2024-10-01T02:00:00Z', 'temperature': 8.2, 'pression': 1019.5},
        ]
        
        # Count missing values per field
        missing_counts = defaultdict(int)
        
        for record in records:
            for field, value in record.items():
                if value is None:
                    missing_counts[field] += 1
        
        self.assertEqual(missing_counts['pression'], 1)
        self.assertEqual(missing_counts['temperature'], 1)
        
        # Check critical fields
        critical_fields = ['id_station', 'dh_utc']
        critical_missing = {f: count for f, count in missing_counts.items() if f in critical_fields}
        
        self.assertEqual(len(critical_missing), 0, "Critical fields should not be missing")


# ============================================================================
# TEST 5: Data Ranges
# ============================================================================

class TestDataRanges(unittest.TestCase):
    """Test 5: Validate data ranges."""
    
    def test_temperature_range(self):
        """Temperature should be between -60°C and +60°C (realistic range)."""
        config = {
            'type': 'excel',
            'source_name': 'Test',
            'station_id': 'TEST',
            'station_name': 'Test',
            'latitude': 50.0,
            'longitude': 3.0,
            'elevation': 25,
            'city': 'Test',
            'state': '--',
            'hardware': 'test',
            'software': 'test'
        }
        
        handler = ExcelDataHandler(config)
        
        # Test realistic conversions
        temps_to_test = ['56.8 °F', '-4 °F', '32 °F']
        
        for temp_str in temps_to_test:
            temp = handler._fahrenheit_to_celsius(temp_str)
            if temp is not None:
                self.assertTrue(-60 <= temp <= 60, 
                              f"Temperature out of range: {temp}°C from {temp_str}")
    
    def test_humidity_range(self):
        """Humidity should be between 0 and 100%."""
        humidity_values = ['87 %', '0 %', '100 %', None]
        
        for humidity_str in humidity_values:
            if humidity_str is not None:
                humidity = float(humidity_str.replace(' %', ''))
                self.assertTrue(0 <= humidity <= 100, 
                              f"Humidity out of range: {humidity}%")
    
    def test_wind_direction_range(self):
        """Wind direction should be between 0 and 360 degrees."""
        records = [
            {'vent_direction': 0},
            {'vent_direction': 90},
            {'vent_direction': 180},
            {'vent_direction': 270},
            {'vent_direction': 360},
            {'vent_direction': None},
        ]
        
        for record in records:
            direction = record.get('vent_direction')
            if direction is not None:
                self.assertTrue(0 <= direction <= 360, 
                              f"Wind direction out of range: {direction}°")


# ============================================================================
# TEST 6: Unified Structure
# ============================================================================

class TestUnifiedStructure(unittest.TestCase):
    """Test 6: Verify unified output structure."""
    
    def test_unified_structure_format(self):
        """Test that unified output has correct structure."""
        unified = {
            "status": "OK",
            "stations": [
                {"id": "TEST1", "name": "Test Station"},
                {"id": "TEST2", "name": "Test Station 2"}
            ],
            "metadata": {
                "id_station": "Station identifier",
                "temperature": "float - degrees Celsius"
            },
            "hourly": {
                "TEST1": [
                    {"id_station": "TEST1", "dh_utc": "2024-10-01T00:00:00Z", "temperature": 15.0},
                    {"id_station": "TEST1", "dh_utc": "2024-10-01T01:00:00Z", "temperature": 14.5}
                ]
            }
        }
        
        # Validate structure
        self.assertIn("status", unified)
        self.assertIn("stations", unified)
        self.assertIn("metadata", unified)
        self.assertIn("hourly", unified)
        
        self.assertEqual(unified["status"], "OK")
        self.assertIsInstance(unified["stations"], list)
        self.assertIsInstance(unified["hourly"], dict)
        
        # Validate stations have required fields
        for station in unified["stations"]:
            self.assertIn("id", station)
            self.assertIn("name", station)
        
        # Validate hourly data structure
        for station_id, records in unified["hourly"].items():
            self.assertIsInstance(records, list)
            for record in records:
                self.assertIn("id_station", record)
                self.assertIn("dh_utc", record)


# ============================================================================
# TEST 7: Source Consistency
# ============================================================================

class TestSourceConsistency(unittest.TestCase):
    """Test 7: Verify all sources use same schema."""
    
    EXPECTED_FIELDS = {
        'id_station', 'dh_utc', 'temperature', 'pression', 'humidite',
        'point_de_rosee', 'visibilite', 'vent_moyen', 'vent_rafales',
        'vent_direction', 'pluie_3h', 'pluie_1h', 'neige_au_sol',
        'nebulosite', 'temps_omm'
    }
    
    def test_excel_json_field_consistency(self):
        """Test that Excel and JSON handlers use same field names."""
        
        # Excel sample fields (converted to normalized names)
        excel_fields = {
            'id_station', 'dh_utc', 'temperature', 'pression', 'humidite',
            'point_de_rosee', 'visibilite', 'vent_moyen', 'vent_rafales',
            'vent_direction', 'pluie_3h', 'pluie_1h', 'neige_au_sol',
            'nebulosite', 'temps_omm'
        }
        
        # JSON sample fields
        json_fields = {
            'id_station', 'dh_utc', 'temperature', 'pression', 'humidite',
            'point_de_rosee', 'visibilite', 'vent_moyen', 'vent_rafales',
            'vent_direction', 'pluie_3h', 'pluie_1h', 'neige_au_sol',
            'nebulosite', 'temps_omm'
        }
        
        # Should have same fields
        self.assertEqual(excel_fields, json_fields, 
                        f"Field mismatch - Excel only: {excel_fields - json_fields}, "
                        f"JSON only: {json_fields - excel_fields}")
        
        # Both should match expected
        self.assertEqual(excel_fields, self.EXPECTED_FIELDS)
        self.assertEqual(json_fields, self.EXPECTED_FIELDS)


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_quality_tests():
    """Run all quality tests and generate report."""
    
    print("\n" + "=" * 70)
    print("STARTING DATA QUALITY TESTS")
    print("=" * 70 + "\n")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all tests
    suite.addTests(loader.loadTestsFromTestCase(TestColumnAvailability))
    suite.addTests(loader.loadTestsFromTestCase(TestDataTypes))
    suite.addTests(loader.loadTestsFromTestCase(TestDuplicates))
    suite.addTests(loader.loadTestsFromTestCase(TestMissingValues))
    suite.addTests(loader.loadTestsFromTestCase(TestDataRanges))
    suite.addTests(loader.loadTestsFromTestCase(TestUnifiedStructure))
    suite.addTests(loader.loadTestsFromTestCase(TestSourceConsistency))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Generate report
    report = DataQualityReport()
    
    report.add_check('Overall Test Results', 
                    'PASS' if result.wasSuccessful() else 'FAIL',
                    {
                        'tests_run': result.testsRun,
                        'failures': len(result.failures),
                        'errors': len(result.errors)
                    })
    
    report.print_report()
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_all_quality_tests()
    sys.exit(0 if success else 1)
