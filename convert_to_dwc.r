#!/usr/bin/env Rscript
# Transform data to Darwin Core Archive
#
# Schema reference: https://example.org/data-sample
# Event hierarchy:
#   - Cruise events (deployment/recovery) with footprintWKT
#   - Station events (points) as children

library(tidyverse)
library(readxl)

# Read source data from Excel
station_df <- read_excel("Data_sample.xlsx", sheet = "Station")
cpue_df <- read_excel("Data_sample.xlsx", sheet = "CPUE")
measurements_df <- read_excel("Data_sample.xlsx", sheet = "Measurements")

cat("Loaded data:\n")
cat("  Station:", nrow(station_df), "rows\n")
cat("  CPUE:", nrow(cpue_df), "rows\n")
cat("  Measurements:", nrow(measurements_df), "rows\n")

# ============================================================================
# Create parent Cruise events (deployment and recovery)
# Schema: Cruise events have footprintWKT from all stations
# ============================================================================

cruise_events <- station_df %>%
  group_by(cruise_id, type) %>%
  summarise(
    eventDate = first(datetime),
    # Build LINESTRING from all start and end coordinates
    coords = paste(
      na.omit(paste(lon_start, lat_start)),
      na.omit(paste(lon_end, lat_end)),
      collapse = ", "
    ),
    .groups = "drop"
  ) %>%
  mutate(
    eventID = paste0(cruise_id, "_", type),
    eventType = paste(type, "cruise"),
    parentEventID = NA_character_,
    footprintWKT = paste0("LINESTRING (", coords, ")")
  ) %>%
  select(eventID, eventDate, eventType, parentEventID, footprintWKT)

cat("\nCreated", nrow(cruise_events), "parent cruise events\n")

# ============================================================================
# Transform Station -> Event core (as point locations)
# Schema class: Station -> dwc:Event
# Schema slot: station -> dwc:eventID and dwc:locationID
# ============================================================================

station_events <- station_df %>%
  mutate(
    # Create parentEventID based on cruise_id and type
    parentEventID = paste0(cruise_id, "_", type)
  ) %>%
  transmute(
    eventID = station,                    # Schema slot: station -> dwc:eventID
    locationID = station,                 # Schema slot: station -> dwc:locationID
    parentEventID = parentEventID,
    eventDate = datetime,                 # Schema slot: datetime -> dwc:eventDate
    eventType = type,                     # Schema slot: type -> dwc:eventType
    decimalLatitude = lat_start,          # Schema slot: lat_start -> dwc:decimalLatitude (point)
    decimalLongitude = lon_start,         # Schema slot: lon_start -> dwc:decimalLongitude (point)
    minimumDepthInMeters = depth,         # Schema slot: depth -> dwc:minimumDepthInMeters
    maximumDepthInMeters = depth,         # Schema slot: depth -> dwc:maximumDepthInMeters
    eventRemarks = notes,                 # Schema slot: notes -> dwc:eventRemarks
    recordedBy = participants             # Schema slot: participants -> dwc:recordedBy
  )

cat("Created", nrow(station_events), "station events\n")

# Combine cruise and station events
event_core <- bind_rows(cruise_events, station_events)

cat("Total events in core:", nrow(event_core), "\n")

# ============================================================================
# Transform CPUE -> Occurrence extension
# Schema class: CPUE -> dwc:Occurrence
# ============================================================================

occurrence_ext <- cpue_df %>%
  mutate(
    occurrenceID = paste(Station, Pot_ID, Species, row_number(), sep = "_")
  ) %>%
  transmute(
    occurrenceID = occurrenceID,
    eventID = Station,
    vernacularName = Species,
    individualCount = Catch,
    basisOfRecord = "HumanObservation",
    occurrenceRemarks = Notes
  )

cat("\nCreated Occurrence extension from CPUE with", nrow(occurrence_ext), "occurrences\n")

# ============================================================================
# Transform Measurements -> Occurrence extension
# Schema class: Measurements -> dwc:Occurrence
# ============================================================================

measurement_occurrences <- measurements_df %>%
  mutate(
    occurrenceID = paste("MEAS", Station, Species, row_number(), sep = "_"),
    # Combine barotrauma and notes
    occurrenceRemarks = case_when(
      !is.na(Barotrauma) & !is.na(Notes) ~ paste0("Barotrauma: ", Barotrauma, "; ", Notes),
      !is.na(Barotrauma) ~ paste0("Barotrauma: ", Barotrauma),
      !is.na(Notes) ~ Notes,
      TRUE ~ NA_character_
    )
  ) %>%
  transmute(
    occurrenceID = occurrenceID,
    eventID = Station,
    vernacularName = Species,
    sex = Sex,
    basisOfRecord = "HumanObservation",
    occurrenceRemarks = occurrenceRemarks
  )

cat("Created Occurrence extension from Measurements with", nrow(measurement_occurrences), "occurrences\n")

# Combine occurrences
occurrence_combined <- bind_rows(occurrence_ext, measurement_occurrences)

cat("Combined occurrences:", nrow(occurrence_combined), "total\n")

# ============================================================================
# Create MeasurementOrFact extension
# ============================================================================

# Organism measurements from Measurements sheet
measurements_with_id <- measurements_df %>%
  mutate(
    occurrenceID = paste("MEAS", Station, Species, row_number(), sep = "_")
  )

# Total length
length_mof <- measurements_with_id %>%
  filter(!is.na(TL_mm)) %>%
  transmute(
    occurrenceID = occurrenceID,
    measurementType = "total length",
    measurementValue = as.character(TL_mm),
    measurementUnit = "mm",
    measurementUnitID = "http://qudt.org/vocab/unit/MilliM"
  )

# Weight (recorded)
weight_recorded_mof <- measurements_with_id %>%
  filter(!is.na(Wt_g_recorded)) %>%
  transmute(
    occurrenceID = occurrenceID,
    measurementType = "weight (recorded)",
    measurementValue = as.character(Wt_g_recorded),
    measurementUnit = "g",
    measurementUnitID = "http://qudt.org/vocab/unit/GM"
  )

# Scale tare weight
tare_mof <- measurements_with_id %>%
  filter(!is.na(scale_tare_g)) %>%
  transmute(
    occurrenceID = occurrenceID,
    measurementType = "scale tare weight",
    measurementValue = as.character(scale_tare_g),
    measurementUnit = "g",
    measurementUnitID = "http://qudt.org/vocab/unit/GM"
  )

# Weight (calculated)
weight_mof <- measurements_with_id %>%
  filter(!is.na(Wt_g)) %>%
  transmute(
    occurrenceID = occurrenceID,
    measurementType = "weight",
    measurementValue = as.character(Wt_g),
    measurementUnit = "g",
    measurementUnitID = "http://qudt.org/vocab/unit/GM"
  )

# Retained status as measurement
# Schema slot: retained -> dwc:measurementValue (transformation_type: pivot_to_mof)
retained_mof <- measurements_with_id %>%
  filter(!is.na(Retained)) %>%
  transmute(
    occurrenceID = occurrenceID,
    measurementType = "retained",
    measurementValue = as.character(Retained),
    measurementUnit = NA_character_
  )

# Environmental measurements from Station sheet
# Wind speed
wind_speed_mof <- station_df %>%
  filter(!is.na(wind_speed)) %>%
  transmute(
    eventID = station,
    measurementType = "wind speed",
    measurementValue = as.character(wind_speed),
    measurementUnit = "kn",
    measurementUnitID = "http://qudt.org/vocab/unit/KN"
  )

# Wind direction
wind_dir_mof <- station_df %>%
  filter(!is.na(wind_dir)) %>%
  transmute(
    eventID = station,
    measurementType = "wind direction",
    measurementValue = as.character(wind_dir),
    measurementUnit = NA_character_
  )

# Wave height
wave_height_mof <- station_df %>%
  filter(!is.na(wave_height)) %>%
  transmute(
    eventID = station,
    measurementType = "wave height",
    measurementValue = as.character(wave_height),
    measurementUnit = NA_character_
  )

# Cloud cover
cloud_cover_mof <- station_df %>%
  filter(!is.na(cloud_cover_10th)) %>%
  transmute(
    eventID = station,
    measurementType = "cloud cover",
    measurementValue = as.character(cloud_cover_10th),
    measurementUnit = "tenths"
  )

# Ropeless gear ID
ropeless_mof <- station_df %>%
  filter(!is.na(ropeless_id)) %>%
  transmute(
    eventID = station,
    measurementType = "ropeless gear ID",
    measurementValue = as.character(ropeless_id),
    measurementUnit = NA_character_
  )

# Cruise ID as measurement
# Schema slot: cruise_id -> dwc:measurementValue (transformation_type: pivot_to_mof)
cruise_id_mof <- station_df %>%
  filter(!is.na(cruise_id)) %>%
  transmute(
    eventID = station,
    measurementType = "cruise ID",
    measurementValue = as.character(cruise_id),
    measurementUnit = NA_character_
  )

# Gear measurements from CPUE
# Pot position
pot_position_mof <- cpue_df %>%
  filter(!is.na(Pot_position)) %>%
  transmute(
    eventID = Station,
    measurementType = "pot position",
    measurementValue = as.character(Pot_position),
    measurementUnit = NA_character_
  )

# Pot ID
pot_id_mof <- cpue_df %>%
  filter(!is.na(Pot_ID)) %>%
  transmute(
    eventID = Station,
    measurementType = "pot ID",
    measurementValue = as.character(Pot_ID),
    measurementUnit = NA_character_
  )

# Distance category (near/far) from CPUE
near_far_cpue_mof <- cpue_df %>%
  filter(!is.na(Near_Far)) %>%
  transmute(
    eventID = Station,
    measurementType = "distance category",
    measurementValue = as.character(Near_Far),
    measurementUnit = NA_character_
  )

# Distance category from Measurements
near_far_meas_mof <- measurements_df %>%
  filter(!is.na(`Near/Far`)) %>%
  transmute(
    eventID = Station,
    measurementType = "distance category",
    measurementValue = as.character(`Near/Far`),
    measurementUnit = NA_character_
  )

# Combine all measurements
measurement_or_fact <- bind_rows(
  length_mof,
  weight_recorded_mof,
  tare_mof,
  weight_mof,
  retained_mof,
  wind_speed_mof,
  wind_dir_mof,
  wave_height_mof,
  cloud_cover_mof,
  ropeless_mof,
  cruise_id_mof,
  pot_position_mof,
  pot_id_mof,
  near_far_cpue_mof,
  near_far_meas_mof
)

cat("\nCreated MeasurementOrFact extension with", nrow(measurement_or_fact), "measurements\n")

# ============================================================================
# Write outputs
# ============================================================================

write_csv(event_core, "outputs/r_dwc_event.csv", na = "")
write_csv(occurrence_combined, "outputs/r_dwc_occurrence.csv", na = "")
write_csv(measurement_or_fact, "outputs/r_dwc_measurementorfact.csv", na = "")
