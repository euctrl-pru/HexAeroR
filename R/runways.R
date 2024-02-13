library(dplyr)
library(h3jsr)
library(purrr)
library(tidyr)
library(sf)
library(arrow)
library(tidyverse)

# Function to load dataset
load_dataset <- function(name, datatype) {
  file_path <- file.path(".", "data", datatype, name)
  #file_path <- system.file("HexAeroR", file_path, package = "HexAeroR")
  return(arrow::read_parquet(file_path))
}

add_statevector_id <- function(df) {
  # Add a new column 'statevector_id' with a unique ID for each row based on its position
  df <- df %>%
    mutate(statevector_id = row_number())
  return(df)
}

add_hex_ids <- function(df, longitude_col = 'lon', latitude_col = 'lat', resolutions) {
  # Check for required packages
  #if (!requireNamespace("h3jsr", quietly = TRUE) || !requireNamespace("sf", quietly = TRUE)) {
  #  stop("The h3jsr and sf packages are required but not installed.")
  #}

  # Convert the dataframe to an sf object with point geometry
  points <- sf::st_sfc(lapply(1:nrow(df), function(i) {
    sf::st_point(c(df[[longitude_col]][i], df[[latitude_col]][i]))
  }), crs = 4326)
  sf_points <- sf::st_sf(geometry = points)

  # Generate H3 addresses for all points across the specified resolutions
  hex_data <- point_to_cell(input = sf_points, res = resolutions, simple = FALSE)

  # Rename columns
  col_names <- paste0("hex_id_", resolutions)
  # Check if adjustment is needed based on the actual output structure of hex_data
  if (ncol(hex_data) == length(resolutions)) {
    names(hex_data) <- col_names
  } else {
    warning("The structure of hex_data does not match the expected number of resolutions.")
  }

  # Bind the H3 address columns back to the original dataframe
  df_with_hex <- bind_cols(df, hex_data)

  return(df_with_hex)
}

convert_baroalt_in_m_to_ft_and_FL <- function(df, baroaltitude_col = 'baroaltitude') {
  # Converts barometric altitudes from meters to feet and flight levels
  df %>%
    mutate(
      baroaltitude_ft = .[[baroaltitude_col]] * 3.28084,
      baroaltitude_fl = baroaltitude_ft / 100
    )
}

filter_low_altitude_statevectors <- function(df, baroalt_ft_col = 'baroaltitude_ft', threshold = 5000) {
  # Filters out aircraft states below a specified altitude threshold
  df %>%
    filter(.[[baroalt_ft_col]] < threshold)
}


identify_potential_airports <- function(df, track_id_col = 'id', hex_id_col = 'hex_id', apt_types = c('large_airport', 'medium_airport')) {
  # Load airport data using the provided load_dataset function
  airports_df <- load_dataset('airport_hex_res_5_radius_15_nm.parquet', 'airport_hex') %>%
    filter(type %in% apt_types) %>%
    rename(apt_id = id)

  # Merge aircraft states with airport data based on hex ID (resolution 5)
  arr_dep_apt <- df %>%
    inner_join(airports_df, by = setNames(hex_id_col, 'hex_id_5'),  relationship = "many-to-many") %>%
    mutate(time = as.POSIXct(time), # Convert the 'time' column to POSIXct datetime format
           segment_status = '') # Initialize the 'segment_status' column with an empty string

  # For each group, identify start and end points based on time

  arr_dep_apt <- arr_dep_apt %>%
    group_by(!!sym(track_id_col), ident) %>%
    mutate(segment_status = case_when(
      row_number() == 1 ~ 'start',
      row_number() == n() ~ 'end',
      TRUE ~ as.character(segment_status)
    )) %>%
    ungroup()

  # Filter to only include 'start' or 'end'
  filtered_df <- arr_dep_apt %>%
    filter(segment_status %in% c('start', 'end'))

  # Separate DataFrames for 'start' and 'end'
  start_df <- filtered_df %>%
    filter(segment_status == 'start') %>%
    select(-segment_status) %>%
    rename_with(~paste('start', ., sep = '_'), -c(!!sym(track_id_col), 'ident'))

  end_df <- filtered_df %>%
    filter(segment_status == 'end') %>%
    select(-segment_status) %>%
    rename_with(~paste('end', ., sep = '_'), -c(!!sym(track_id_col), 'ident'))

  # Merge the start and end DataFrames on 'id' and 'ident'
  apt_detections_df <- start_df %>%
    full_join(end_df, by = c(track_id_col, 'ident'), na_matches = "never")

  # Select core columns and adjust as needed to match specific requirements
  core_columns <- c(track_id_col, 'ident', 'start_time', 'start_statevector_id', 'end_time', 'end_statevector_id')
  apt_detections_df <- apt_detections_df %>%
    select(all_of(core_columns))

  return(apt_detections_df)
}

identify_runways_from_low_trajectories <- function(apt_detections_df, df_f_low_alt) {

  # Step 0: Creation of an ID & renaming cols
  apt_detections_df <- apt_detections_df %>%
    mutate(apt_detection_id = paste(id, row_number() - 1, sep = "_")) %>%
    select(id, apt_det_ident = ident, apt_det_start_time = start_time, apt_det_end_time = end_time, apt_det_id = apt_detection_id)

  # Step 1: Convert datetime columns to datetime format if they are not already
  apt_detections_df <- apt_detections_df %>%
    mutate(apt_det_start_time = ymd_hms(apt_det_start_time),
           apt_det_end_time = ymd_hms(apt_det_end_time))

  # Step 2: Merge the data frames on 'id'
  merged_df <- inner_join(df_f_low_alt, apt_detections_df, by = "id", relationship = "many-to-many")

  # Step 3: Filter rows where 'time' is between 'apt_det_start_time' and 'apt_det_end_time'
  result_df <- merged_df %>%
    filter(time >= apt_det_start_time & time <= apt_det_end_time)

  # Define the match_runways_to_hex function
  match_runways_to_hex <- function(df_low, .apt_det_id, .apt) {
    df_single <- df_low %>%
      filter(apt_det_id == .apt_det_id) %>%
      select(apt_det_id, id, time, lat, lon, hex_id_11, baroaltitude_fl)

    core_cols_rwy <- c("id", "airport_ref", "airport_ident", "gate_id", "hex_id", "gate_id_nr", "le_ident", "he_ident")

    df_rwys <- load_dataset(name = paste0(.apt, ".parquet"), datatype = "runway_hex") %>%
      select(all_of(core_cols_rwy)) %>%
      rename(c('id_apt' = 'id'))

    df_hex_rwy <- left_join(df_single, df_rwys, by = c("hex_id_11" = "hex_id"))

    result <- df_hex_rwy %>%
      group_by(apt_det_id, id, airport_ident, gate_id, le_ident, he_ident) %>%
      summarise(min_time = min(time), max_time = max(time), .groups = "drop") %>%
      arrange(min_time) %>%
      filter(!is.na(airport_ident)) %>%
      select(-c(apt_det_id, id,))

    return(result)
  }

  # Step 5: Apply match_runways_to_hex for each row in apt_detections_df
  apt_detections_df <- apt_detections_df %>%
    rowwise() %>%
    mutate(matched_runways = list(match_runways_to_hex(result_df, apt_det_id, apt_det_ident))) %>%
    ungroup()

  # Unnest the matched_runways column to expand the nested data frames into a single frame
  final_df <- apt_detections_df %>%
    unnest(matched_runways)

  final_df <- final_df %>%
    arrange(id, apt_det_id, airport_ident, min_time) %>%
    group_by(id, apt_det_id, airport_ident, le_ident, he_ident) %>%
    mutate(min_time = ymd_hms(min_time),
           max_time = ymd_hms(max_time),
           time_diff = as.numeric(difftime(lead(min_time), min_time, units = "mins")),
           new_det_id = cumsum(ifelse(is.na(time_diff) | time_diff > 40, 1, 0)),
           rwy_det_id = paste(apt_det_id, new_det_id, sep = "_")) %>%
    ungroup() %>%
    select(c('id', 'apt_det_id', 'rwy_det_id', 'airport_ident', 'gate_id', 'le_ident', 'he_ident', 'min_time', 'max_time')) %>%
    rename(c(min = min_time, max = max_time))

  return(final_df)
}

manipulate_df_and_determine_arrival_departure <- function(df) {
  # Copying the DataFrame
  result <- df

  # Cleaning gate_id and adding new columns
  result <- result %>%
    mutate(
      gate_info = map(gate_id, function(gate_id) {
        if (gate_id == "runway_hexagons") {
          list(gate_type = "runway_hexagons", gate_distance_from_rwy_nm = 0)
        } else {
          parts <- str_split(gate_id, "_", simplify = TRUE)
          list(gate_type = paste(parts[1:4], collapse = "_"), gate_distance_from_rwy_nm = as.integer(parts[5]))
        }
      }),
      gate_type = map_chr(gate_info, "gate_type"),
      gate_distance_from_rwy_nm = map_dbl(gate_info, "gate_distance_from_rwy_nm")
    ) %>%
    select(-gate_info)

  # Renaming 'id' to 'id_x'
  result <- result %>%
    rename(id_x = id)

  # Identifying min and max distances
  result <- result %>%
    group_by(id_x, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident) %>%
    mutate(
      min_gate_distance = min(gate_distance_from_rwy_nm),
      max_gate_distance = max(gate_distance_from_rwy_nm)
    ) %>%
    ungroup()

  # Filtering for min and max distance rows
  result_min <- result %>%
    filter(gate_distance_from_rwy_nm == min_gate_distance)

  result_max <- result %>%
    filter(gate_distance_from_rwy_nm == max_gate_distance)

  # Preparing the final DataFrame
  cols_of_interest <- c("id_x", "apt_det_id", "rwy_det_id", "airport_ident", "le_ident", "he_ident", "min_gate_distance", "max_gate_distance", "gate_distance_from_rwy_nm")

  result_min <- result_min %>%
    select(all_of(cols_of_interest)) %>%
    rename(
      time_entry_min_distance = min_gate_distance,
      min_gate_distance_from_rwy_nm = gate_distance_from_rwy_nm
    )

  result_max <- result_max %>%
    select(all_of(cols_of_interest)) %>%
    rename(
      time_entry_max_distance = max_gate_distance,
      max_gate_distance_from_rwy_nm = gate_distance_from_rwy_nm
    )

  det <- full_join(result_min, result_max, by = c("id_x", "apt_det_id", "rwy_det_id", "airport_ident", "le_ident", "he_ident"))

  # Compute the status based on time difference
  det <- det %>%
    mutate(
      time_since_minimum_distance_s = as.numeric(difftime(time_entry_min_distance, time_entry_max_distance, units = "secs")),
      status = case_when(
        time_since_minimum_distance_s > 0 ~ "arrival",
        TRUE ~ "departure"
      )
    ) %>%
    select(id_x, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident, status)

  # Aggregating data
  result <- result %>%
    group_by(id_x, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident, gate_type) %>%
    summarise(
      entry_time_approach_area = min(gate_distance_from_rwy_nm),
      exit_time_approach_area = max(gate_distance_from_rwy_nm),
      intersected_subsections = n(),
      minimal_distance_runway = min(gate_distance_from_rwy_nm),
      maximal_distance_runway = max(gate_distance_from_rwy_nm)
    ) %>%
    ungroup()

  list(result = result, det = det)
}




df <- load_dataset('trajectories.parquet','test_data') %>%
  mutate(
    id = paste0(icao24,'-', callsign, '-', date(time))
  ) %>%
  select(c('id', 'time', 'icao24', 'callsign', 'lat', 'lon', 'baroaltitude'))


track_id_col = 'id'
longitude_col = 'lon'
latitude_col = 'lat'
baroaltitude_col = 'baroaltitude'

df_w_id <- add_statevector_id(df)

df_w_hex <-
  add_hex_ids(
    df_w_id,
    longitude_col = longitude_col,
    latitude_col = latitude_col,
    resolutions = c(5, 11)
  )

df_w_baroalt_ft_fl <-
  convert_baroalt_in_m_to_ft_and_FL(df_w_hex, baroaltitude_col = baroaltitude_col)

df_f_low_alt <-
  filter_low_altitude_statevectors(df_w_baroalt_ft_fl,
                                   baroalt_ft_col = 'baroaltitude_ft',
                                   threshold = 5000)

apt_detections_df <-
  identify_potential_airports(
    df_f_low_alt,
    track_id_col = track_id_col,
    hex_id_col = 'hex_id',
    apt_types = c('large_airport')
  )

rwy_detections_df <-
  identify_runways_from_low_trajectories(apt_detections_df, df_f_low_alt)

manipulate_df_and_determine_arrival_departure(rwy_detections_df)
