#!/usr/bin/env python3
"""
Transform data to Darwin Core Archive

Schema reference: https://example.org/data-sample
Event hierarchy: 
  - Cruise events (deployment/recovery) with footprintWKT
  - Station events (points) as children
"""

import pandas as pd
import numpy as np

# Read source data from Excel
station_df = pd.read_excel('Data_sample.xlsx', sheet_name='Station')
cpue_df = pd.read_excel('Data_sample.xlsx', sheet_name='CPUE')
measurements_df = pd.read_excel('Data_sample.xlsx', sheet_name='Measurements')

print(f"Loaded data:")
print(f"  Station: {len(station_df)} rows")
print(f"  CPUE: {len(cpue_df)} rows")
print(f"  Measurements: {len(measurements_df)} rows")

# ============================================================================
# Create parent Cruise events (deployment and recovery)
# Schema: Cruise events have footprintWKT from all stations
# ============================================================================

# Get unique cruise_id and types
cruise_types = station_df.groupby(['cruise_id', 'type']).agg({
    'lon_start': lambda x: list(x),
    'lat_start': lambda x: list(x),
    'lon_end': lambda x: list(x),
    'lat_end': lambda x: list(x),
    'datetime': 'first'
}).reset_index()

cruise_events = []
for idx, row in cruise_types.iterrows():
    # Build LINESTRING from all station start/end points for this cruise+type
    coords = []
    for lon_s, lat_s, lon_e, lat_e in zip(row['lon_start'], row['lat_start'], 
                                            row['lon_end'], row['lat_end']):
        if pd.notna(lon_s) and pd.notna(lat_s):
            coords.append(f"{lon_s} {lat_s}")
        if pd.notna(lon_e) and pd.notna(lat_e):
            coords.append(f"{lon_e} {lat_e}")
    
    footprint = f"LINESTRING ({', '.join(coords)})" if coords else None
    
    cruise_events.append({
        'eventID': f"{int(row['cruise_id'])}_{row['type']}",
        'eventDate': row['datetime'],
        'eventType': f"{row['type']} cruise",
        'parentEventID': None,
        'footprintWKT': footprint
    })

cruise_event_df = pd.DataFrame(cruise_events)

print(f"\nCreated {len(cruise_event_df)} parent cruise events")

# ============================================================================
# Transform Station -> Event core (as point locations)
# Schema class: Station -> dwc:Event
# Schema slot: station -> dwc:eventID and dwc:locationID
# ============================================================================

station_events = station_df.copy()

# Create parentEventID based on cruise_id and type
station_events['parentEventID'] = (station_events['cruise_id'].astype(int).astype(str) + '_' + 
                                    station_events['type'])

station_events = station_events.rename(columns={
    'station': 'eventID',           # Schema slot: station -> dwc:eventID
    'datetime': 'eventDate',        # Schema slot: datetime -> dwc:eventDate
    'type': 'eventType',            # Schema slot: type -> dwc:eventType
    'lat_start': 'decimalLatitude', # Schema slot: lat_start -> dwc:decimalLatitude (point)
    'lon_start': 'decimalLongitude',# Schema slot: lon_start -> dwc:decimalLongitude (point)
    'depth': 'minimumDepthInMeters',# Schema slot: depth -> dwc:minimumDepthInMeters
    'notes': 'eventRemarks',        # Schema slot: notes -> dwc:eventRemarks
    'participants': 'recordedBy'    # Schema slot: participants -> dwc:recordedBy
})

# Add locationID (same as eventID for stations)
# Schema slot: station -> dwc:locationID
station_events['locationID'] = station_events['eventID']

# Add maximumDepthInMeters (same as minimum for single measurements)
station_events['maximumDepthInMeters'] = station_events['minimumDepthInMeters']

# Select final columns for Station events
station_events = station_events[[
    'eventID', 'locationID', 'parentEventID', 'eventDate', 'eventType', 
    'decimalLatitude', 'decimalLongitude', 'minimumDepthInMeters', 
    'maximumDepthInMeters', 'eventRemarks', 'recordedBy'
]]

# Combine cruise and station events
event_core = pd.concat([cruise_event_df, station_events], ignore_index=True)

print(f"Created {len(station_events)} station events")
print(f"Total events in core: {len(event_core)}")

# ============================================================================
# Transform CPUE -> Occurrence extension
# Schema class: CPUE -> dwc:Occurrence
# ============================================================================

cpue_df['occurrenceID'] = (cpue_df['Station'].astype(str) + '_' + 
                            cpue_df['Pot_ID'].astype(str) + '_' + 
                            cpue_df['Species'].astype(str) + '_' + 
                            (cpue_df.index + 1).astype(str))

occurrence_ext = cpue_df[['occurrenceID', 'Station', 'Species', 'Catch', 'Notes']].copy()

occurrence_ext = occurrence_ext.rename(columns={
    'Station': 'eventID',
    'Species': 'vernacularName',
    'Catch': 'individualCount',
    'Notes': 'occurrenceRemarks'
})

occurrence_ext['basisOfRecord'] = 'HumanObservation'

print(f"\nCreated Occurrence extension from CPUE with {len(occurrence_ext)} occurrences")

# ============================================================================
# Transform Measurements -> Occurrence extension
# Schema class: Measurements -> dwc:Occurrence
# ============================================================================

measurements_df['occurrenceID'] = ('MEAS_' + 
                                   measurements_df['Station'].astype(str) + '_' + 
                                   measurements_df['Species'].astype(str) + '_' + 
                                   (measurements_df.index + 1).astype(str))

# Combine barotrauma and notes
def combine_remarks(row):
    parts = []
    if pd.notna(row.get('Barotrauma')):
        parts.append(f"Barotrauma: {row['Barotrauma']}")
    if pd.notna(row.get('Notes')):
        parts.append(str(row['Notes']))
    return '; '.join(parts) if parts else None

measurements_df['combined_remarks'] = measurements_df.apply(combine_remarks, axis=1)

measurement_occurrences = measurements_df[['occurrenceID', 'Station', 'Species', 
                                           'Sex', 'combined_remarks']].copy()

measurement_occurrences = measurement_occurrences.rename(columns={
    'Station': 'eventID',
    'Species': 'vernacularName',
    'Sex': 'sex',
    'combined_remarks': 'occurrenceRemarks'
})

measurement_occurrences['basisOfRecord'] = 'HumanObservation'

print(f"Created Occurrence extension from Measurements with {len(measurement_occurrences)} occurrences")

# Combine occurrences
occurrence_combined = pd.concat([occurrence_ext, measurement_occurrences], ignore_index=True)

print(f"Combined occurrences: {len(occurrence_combined)} total")

# ============================================================================
# Create MeasurementOrFact extension
# ============================================================================

measurement_records = []

# Organism measurements from Measurements sheet
for idx, row in measurements_df.iterrows():
    occ_id = row['occurrenceID']
    
    # Total length
    if pd.notna(row['TL_mm']):
        measurement_records.append({
            'occurrenceID': occ_id,
            'measurementType': 'total length',
            'measurementValue': str(row['TL_mm']),
            'measurementUnit': 'mm',
            'measurementUnitID': 'http://qudt.org/vocab/unit/MilliM'
        })
    
    # Weight (recorded)
    if pd.notna(row['Wt_g_recorded']):
        measurement_records.append({
            'occurrenceID': occ_id,
            'measurementType': 'weight (recorded)',
            'measurementValue': str(row['Wt_g_recorded']),
            'measurementUnit': 'g',
            'measurementUnitID': 'http://qudt.org/vocab/unit/GM'
        })
    
    # Scale tare weight
    if pd.notna(row['scale_tare_g']):
        measurement_records.append({
            'occurrenceID': occ_id,
            'measurementType': 'scale tare weight',
            'measurementValue': str(row['scale_tare_g']),
            'measurementUnit': 'g',
            'measurementUnitID': 'http://qudt.org/vocab/unit/GM'
        })
    
    # Weight (calculated)
    if pd.notna(row['Wt_g']):
        measurement_records.append({
            'occurrenceID': occ_id,
            'measurementType': 'weight',
            'measurementValue': str(row['Wt_g']),
            'measurementUnit': 'g',
            'measurementUnitID': 'http://qudt.org/vocab/unit/GM'
        })
    
    # Retained status as measurement
    # Schema slot: retained -> dwc:measurementValue (transformation_type: pivot_to_mof)
    if pd.notna(row['Retained']):
        measurement_records.append({
            'occurrenceID': occ_id,
            'measurementType': 'retained',
            'measurementValue': str(row['Retained']),
            'measurementUnit': ''
        })

# Environmental measurements from Station sheet
for idx, row in station_df.iterrows():
    event_id = row['station']
    
    # Wind speed
    if pd.notna(row['wind_speed']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'wind speed',
            'measurementValue': str(row['wind_speed']),
            'measurementUnit': 'kn',
            'measurementUnitID': 'http://qudt.org/vocab/unit/KN'
        })
    
    # Wind direction
    if pd.notna(row['wind_dir']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'wind direction',
            'measurementValue': str(row['wind_dir']),
            'measurementUnit': ''
        })
    
    # Wave height
    if pd.notna(row['wave_height']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'wave height',
            'measurementValue': str(row['wave_height']),
            'measurementUnit': ''
        })
    
    # Cloud cover
    if pd.notna(row['cloud_cover_10th']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'cloud cover',
            'measurementValue': str(row['cloud_cover_10th']),
            'measurementUnit': 'tenths'
        })
    
    # Ropeless gear ID
    if pd.notna(row['ropeless_id']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'ropeless gear ID',
            'measurementValue': str(row['ropeless_id']),
            'measurementUnit': ''
        })
    
    # Cruise ID as measurement
    # Schema slot: cruise_id -> dwc:measurementValue (transformation_type: pivot_to_mof)
    if pd.notna(row['cruise_id']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'cruise ID',
            'measurementValue': str(int(row['cruise_id'])),
            'measurementUnit': ''
        })

# Gear measurements from CPUE
for idx, row in cpue_df.iterrows():
    event_id = row['Station']
    
    # Pot position
    if pd.notna(row['Pot_position']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'pot position',
            'measurementValue': str(row['Pot_position']),
            'measurementUnit': ''
        })
    
    # Pot ID
    if pd.notna(row['Pot_ID']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'pot ID',
            'measurementValue': str(row['Pot_ID']),
            'measurementUnit': ''
        })
    
    # Distance category (near/far)
    if pd.notna(row['Near_Far']):
        measurement_records.append({
            'eventID': event_id,
            'measurementType': 'distance category',
            'measurementValue': str(row['Near_Far']),
            'measurementUnit': ''
        })

# Distance category from Measurements
for idx, row in measurements_df.iterrows():
    if pd.notna(row['Near/Far']):
        measurement_records.append({
            'eventID': row['Station'],
            'measurementType': 'distance category',
            'measurementValue': str(row['Near/Far']),
            'measurementUnit': ''
        })

measurement_or_fact = pd.DataFrame(measurement_records)

print(f"\nCreated MeasurementOrFact extension with {len(measurement_or_fact)} measurements")

# ============================================================================
# Write outputs
# ============================================================================

event_core.to_csv('outputs/py_dwc_event.csv', index=False, na_rep='')
occurrence_combined.to_csv('outputs/py_dwc_occurrence.csv', index=False, na_rep='')
measurement_or_fact.to_csv('outputs/py_dwc_measurementorfact.csv', index=False, na_rep='')
