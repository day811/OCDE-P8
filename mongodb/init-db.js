# mongodb/init-db.js

/**
 * MongoDB Initialization Script - Normalized Schema
 * Creates 3 collections with appropriate indexes
 */

db = db.getSiblingDB('admin');
db = db.getSiblingDB('greencoop_forecast');

print('================================================');
print('MongoDB Initialization - Normalized Schema');
print('================================================');

// Stations collection indexes
db.stations.createIndex({ 'id_station': 1 }, { unique: true, name: 'idx_station_unique' });
db.stations.createIndex({ 'city': 1 }, { name: 'idx_city' });

print('✓ Stations collection indexes created');

// Observations collection indexes
db.observations.createIndex({ 'id_station': 1 }, { name: 'idx_obs_station' });
db.observations.createIndex({ 'dh_utc': -1 }, { name: 'idx_obs_timestamp' });
db.observations.createIndex({ 'id_station': 1, 'dh_utc': -1 }, { name: 'idx_obs_station_time' });
db.observations.createIndex({ '_source': 1 }, { name: 'idx_obs_source' });

print('✓ Observations collection indexes created');

// Schema metadata collection indexes
db.schema_metadata.createIndex({ 'field_name': 1 }, { unique: true, name: 'idx_field_unique' });

print('✓ Schema metadata collection indexes created');

print('================================================');
print('✓ Database initialization complete');
print('================================================');
