# transformation/tests/test_preprocessing.py

import unittest
import json
import tempfile
import yaml
from pathlib import Path
from datetime import datetime
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src/preprocessing'))

from unified_data_pipeline import (
    ExcelDataHandler,
    JSONDataHandler,
    UnifiedDataPipeline
)

# ============================================================================
# EXCEL HANDLER TESTS
# ============================================================================

class TestExcelDataHandler(unittest.TestCase):
    """Test Excel data preprocessing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'type': 'excel',
            'source_name': 'Test Wunderground',
            'station_id': 'TEST123',
            'station_name': 'Test Station',
            'latitude': 50.0,
            'longitude': 3.0,
            'elevation': 25,
            'city': 'Test City',
            'state': '-/-',
            'hardware': 'test_hw',
            'software': 'test_sw',
            'skip_empty_rows': True,
            'skip_header_blank_rows': True
        }
    
    def test_fahrenheit_to_celsius(self):
        """Test Fahrenheit to Celsius conversion."""
        handler = ExcelDataHandler(self.config)
        
        # Test conversion
        result = handler._fahrenheit_to_celsius("32.0 °F")
        self.assertEqual(result, 0.0)
        
        result = handler._fahrenheit_to_celsius("68.0 °F")
        self.assertAlmostEqual(result, 20.0, places=1)
        
        # Test null handling
        result = handler._fahrenheit_to_celsius(None)
        self.assertIsNone(result)
        
        result = handler._fahrenheit_to_celsius("")
        self.assertIsNone(result)
    
    def test_mph_to_kmh(self):
        """Test mph to km/h conversion."""
        handler = ExcelDataHandler(self.config)
        
        result = handler._mph_to_kmh("10.0 mph")
        self.assertAlmostEqual(result, 16.1, places=1)
        
        result = handler._mph_to_kmh(None)
        self.assertIsNone(result)
    
    def test_pressure_conversion(self):
        """Test pressure inches to hPa."""
        handler = ExcelDataHandler(self.config)
        
        result = handler._extract_pressure("29.92 in")
        self.assertAlmostEqual(result, 1013.2, places=0)
        
        result = handler._extract_pressure(None)
        self.assertIsNone(result)
    
    def test_precipitation_conversion(self):
        """Test precipitation inches to mm."""
        handler = ExcelDataHandler(self.config)
        
        result = handler._extract_precipitation("0.1 in")
        self.assertAlmostEqual(result, 2.5, places=1)
        
        result = handler._extract_precipitation("0.00 in")
        self.assertEqual(result, 0.0)
        
        result = handler._extract_precipitation(None)
        self.assertIsNone(result)
    
    def test_wind_direction_conversion(self):
        """Test wind direction abbreviation to degrees."""
        handler = ExcelDataHandler(self.config)
        
        test_cases = {
            'N': 0,
            'NE': 45,
            'E': 90,
            'SE': 135,
            'S': 180,
            'SW': 225,
            'W': 270,
            'NW': 315
        }
        
        for abbr, expected_deg in test_cases.items():
            result = handler._extract_wind_direction_degrees(abbr)
            self.assertEqual(result, expected_deg)
        
        result = handler._extract_wind_direction_degrees(None)
        self.assertIsNone(result)
    
    def test_parse_sheet_date(self):
        """Test sheet name date parsing."""
        handler = ExcelDataHandler(self.config)
        
        # DDMMYY format
        result = handler.parse_sheet_date("011024")
        self.assertEqual(result, "2024-10-01")
        
        result = handler.parse_sheet_date("251224")
        self.assertEqual(result, "2024-12-25")
    
    def test_normalize_row(self):
        """Test row normalization."""
        handler = ExcelDataHandler(self.config)
        
        raw_data = [{
            'Temperature': '56.8 °F',
            'Wind': 'WSW',
            'Speed': '8.2 mph',
            'Pressure': '29.48 in',
            'Humidity': '87 %',
            'Dew Point': '53.1 °F',
            'Gust': '10.4 mph',
            'Time': '00:04:00',
            'Precip. Accum.': '0.00 in',
            'UV': '0',
            '_sheet_date': '2024-10-01',
            '_sheet_name': '011024'
        }]
        
        normalized = handler.normalize(raw_data)
        
        self.assertEqual(len(normalized), 1)
        row = normalized[0]
        
        # Check station metadata
        self.assertEqual(row['id_station'], 'TEST123')
        self.assertEqual(row['station_name'], 'Test Station')
        
        # Check measurements
        self.assertAlmostEqual(row['temperature'], 13.8, places=1)
        self.assertAlmostEqual(row['pression'], 997.5, places=0)
        self.assertEqual(row['humidite'], 87.0)
        self.assertAlmostEqual(row['vent_moyen'], 13.2, places=1)
        self.assertEqual(row['vent_direction'], 247.5)
        
        # Check timestamp
        self.assertEqual(row['dh_utc'], '2024-10-01T00:04:00Z')

# ============================================================================
# JSON HANDLER TESTS
# ============================================================================

class TestJSONDataHandler(unittest.TestCase):
    """Test JSON data preprocessing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'type': 'json',
            'source_name': 'Test InfoClimat',
            'skip_empty_rows': True
        }
    
    def test_to_float_conversion(self):
        """Test safe float conversion."""
        handler = JSONDataHandler(self.config)
        
        self.assertEqual(handler._to_float("12.5"), 12.5)
        self.assertEqual(handler._to_float(12.5), 12.5)
        self.assertEqual(handler._to_float(""), None)
        self.assertEqual(handler._to_float(None), None)
        self.assertEqual(handler._to_float("invalid"), None)
    
    def test_to_int_conversion(self):
        """Test safe int conversion."""
        handler = JSONDataHandler(self.config)
        
        self.assertEqual(handler._to_int("5"), 5)
        self.assertEqual(handler._to_int(5.7), 5)
        self.assertEqual(handler._to_int(""), None)
        self.assertEqual(handler._to_int(None), None)
    
    def test_normalize_infoclimat_row(self):
        """Test InfoClimat row normalization."""
        handler = JSONDataHandler(self.config)
        
        raw_data = [{
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
            'pluie_1h': '0',
            'nebulosite': '7'
        }]
        
        normalized = handler.normalize(raw_data)
        
        self.assertEqual(len(normalized), 1)
        row = normalized[0]
        
        # Check conversions
        self.assertEqual(row['id_station'], '00052')
        self.assertEqual(row['temperature'], 7.6)
        self.assertEqual(row['pression'], 1020.7)
        self.assertEqual(row['humidite'], 89.0)
        self.assertEqual(row['vent_direction'], 90.0)

# ============================================================================
# PIPELINE TESTS
# ============================================================================

class TestUnifiedDataPipeline(unittest.TestCase):
    """Test unified pipeline."""
    
    def setUp(self):
        """Create temporary config file."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_file = os.path.join(self.temp_dir.name, 'test_config.yaml')
        
        config = {
            'sources': {
                'test_excel': {
                    'type': 'excel',
                    'source_name': 'Test Excel',
                    'station_id': 'TEST1',
                    'station_name': 'Test 1',
                    'latitude': 50.0,
                    'longitude': 3.0,
                    'elevation': 25,
                    'city': 'Test',
                    'state': '-/-',
                    'hardware': 'test',
                    'software': 'test',
                    'file_path': '/nonexistent/file.xlsx'
                }
            }
        }
        
        with open(self.config_file, 'w') as f:
            yaml.dump(config, f)
    
    def tearDown(self):
        """Clean up temporary files."""
        self.temp_dir.cleanup()
    
    def test_load_config(self):
        """Test configuration loading."""
        pipeline = UnifiedDataPipeline(self.config_file)
        
        self.assertIn('test_excel', pipeline.config['sources'])
        self.assertEqual(
            pipeline.config['sources']['test_excel']['station_id'],
            'TEST1'
        )
    
    def test_handler_initialization(self):
        """Test handler initialization."""
        pipeline = UnifiedDataPipeline(self.config_file)
        
        self.assertIn('test_excel', pipeline.handlers)
        self.assertIsInstance(pipeline.handlers['test_excel'], ExcelDataHandler)

# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestDataIntegrity(unittest.TestCase):
    """Test data integrity."""
    
    def test_row_filtering_empty_rows(self):
        """Test that empty rows are filtered."""
        config = {
            'type': 'excel',
            'source_name': 'Test',
            'station_id': 'TEST',
            'station_name': 'Test',
            'latitude': 50.0,
            'longitude': 3.0,
            'elevation': 25,
            'city': 'Test',
            'state': '-/-',
            'hardware': 'test',
            'software': 'test',
            'skip_empty_rows': True
        }
        
        handler = ExcelDataHandler(config)
        
        # Mix of filled and empty rows
        raw_data = [
            {'Temperature': '56.8 °F', 'Time': '00:00:00', '_sheet_date': '2024-10-01'},
            {'Temperature': None, 'Time': None, '_sheet_date': '2024-10-01'},  # Empty
            {'Temperature': '57.0 °F', 'Time': '01:00:00', '_sheet_date': '2024-10-01'},
        ]
        
        filtered = handler.filter_empty_rows(raw_data)
        self.assertEqual(len(filtered), 2)  # Should filter out empty row
    
    def test_null_values_handling(self):
        """Test that null values are properly handled."""
        config = {
            'type': 'json',
            'source_name': 'Test',
            'skip_empty_rows': True
        }
        
        handler = JSONDataHandler(config)
        
        raw_data = [{
            'id_station': '00052',
            'dh_utc': '2024-10-01 00:00:00',
            'temperature': '7.6',
            'pression': None,  # Null value
            'humidite': '',    # Empty string
        }]
        
        normalized = handler.normalize(raw_data)
        row = normalized[0]
        
        self.assertEqual(row['temperature'], 7.6)
        self.assertIsNone(row['pression'])
        self.assertIsNone(row['humidite'])

# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    unittest.main(verbosity=2)
