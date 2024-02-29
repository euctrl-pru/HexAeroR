library(dplyr)
library(h3jsr)
library(purrr)
library(tidyr)
library(sf)
library(arrow)
library(tidyverse)

#' Load Dataset from Parquet File
#'
#' This function loads a dataset from a Parquet file located within a specified data type directory.
#' It utilizes the `arrow` package for reading Parquet files efficiently.
#'
#' @param name The name of the dataset file (without extension) to be loaded.
#' @param datatype The type of data or directory under 'data' where the dataset is stored.
#' @return An object of class `tbl_df` representing the dataset loaded from the Parquet file.
#' @importFrom arrow read_parquet
#' @export
load_dataset <- function(name, datatype) {
  file_path <- system.file('data', package = "HexAeroR")
  file_path <- file.path(file_path, datatype, name)
  return(arrow::read_parquet(file_path))
}


#' Add Statevector ID to Dataframe
#'
#' Adds a new column named 'statevector_id' to a dataframe, assigning a unique ID to each row based
#' on its position. This function is useful for uniquely identifying rows without an existing unique identifier.
#'
#' @param df A dataframe to which the 'statevector_id' column will be added.
#' @return A dataframe with an added 'statevector_id' column.
#' @importFrom dplyr mutate row_number
#' @export
add_statevector_id <- function(df) {
  # Add a new column 'statevector_id' with a unique ID for each row based on its position
  df <- df |>
    mutate(statevector_id = row_number())
  return(df)
}


#' Add Hex IDs to Dataframe
#'
#' Converts geographic coordinates in a dataframe to H3 hex IDs at specified resolutions.
#' The function requires longitude and latitude columns and uses the `h3jsr` and `sf` packages
#' to convert these coordinates to H3 hex IDs.
#'
#' @param df Dataframe containing the geographic coordinates.
#' @param longitude_col The name of the column containing longitude values.
#' @param latitude_col The name of the column containing latitude values.
#' @param resolutions A vector of integers specifying the H3 resolution(s) for which hex IDs are generated.
#' @return A dataframe with added columns for H3 hex IDs at specified resolutions.
#' @note This function assumes that the `h3jsr` and `sf` packages are installed and available.
#'       It also converts the dataframe to an `sf` object for processing.
#' @importFrom sf st_sfc st_point st_sf
#' @importFrom purrr lapply
#' @importFrom dplyr bind_cols
#' @export
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


#' Convert Barometric Altitude from Meters to Feet and Flight Levels
#'
#' This function converts barometric altitude values from meters to feet and calculates
#' the corresponding flight level. The conversion and calculation are added as new columns
#' to the dataframe.
#'
#' @param df A dataframe containing the barometric altitude column.
#' @param baroaltitude_col Name of the column containing barometric altitude in meters.
#' Default is 'baroaltitude'.
#' @return A dataframe with two new columns: one for altitude in feet ('baroaltitude_ft')
#' and another for flight level ('baroaltitude_fl').
#' @importFrom dplyr mutate
#' @export
convert_baroalt_in_m_to_ft_and_FL <- function(df, baroaltitude_col = 'baroaltitude') {
  # Converts barometric altitudes from meters to feet and flight levels
  df |>
    mutate(
      baroaltitude_ft = .data[[baroaltitude_col]] * 3.28084,
      baroaltitude_fl = baroaltitude_ft / 100
    )
}

#' Filter Out Low Altitude Aircraft States
#'
#' Filters out rows in a dataframe representing aircraft states below a specified altitude
#' threshold in feet. This is useful for focusing on aircraft at higher operational altitudes.
#'
#' @param df Dataframe containing aircraft state vectors with altitude information.
#' @param baroalt_ft_col Name of the column containing barometric altitude in feet.
#' Default is 'baroaltitude_ft'.
#' @param threshold Altitude threshold in feet below which rows are filtered out.
#' Default is 5000 feet.
#' @return A dataframe with rows below the specified altitude threshold filtered out.
#' @importFrom dplyr filter
#' @export
filter_low_altitude_statevectors <- function(df, baroalt_ft_col = 'baroaltitude_ft', threshold = 5000) {
  # Filters out aircraft states below a specified altitude threshold
  df |>
    filter(.data[[baroalt_ft_col]] < threshold)
}

#' Identify Potential Airport Arrivals and Departures
#'
#' Analyzes aircraft state vectors to identify potential arrivals and departures at airports.
#' It loads airport data, filters by airport type, and merges aircraft states with airport data
#' based on hex IDs. It then identifies the start and end points of each aircraft's track,
#' classifying them as potential arrivals or departures.
#'
#' @param df Dataframe containing aircraft state vectors.
#' @param track_id_col Name of the column containing unique track identifiers for the aircraft.
#' Default is 'id'.
#' @param hex_id_col Name of the column containing hex IDs used for spatial matching with airports.
#' Default is 'hex_id'.
#' @param apt_types Vector of airport types to consider for matching, such as 'large_airport' or
#' 'medium_airport'. Default is `c('large_airport', 'medium_airport')`.
#' @return A dataframe of potential airport arrivals and departures, with start and end points for
#' each track.
#' @importFrom dplyr filter rename inner_join group_by mutate ungroup select full_join rename_with
#' @importFrom tidyr separate
#' @export
identify_potential_airports <- function(df, track_id_col = 'id', hex_id_col = 'hex_id', apt_types = c('large_airport', 'medium_airport')) {
  # Load airport data using the provided load_dataset function
  airports_df <- load_dataset('airport_hex_res_5_radius_15_nm.parquet', 'airport_hex') |>
    filter(type %in% apt_types) |>
    rename(apt_id = id)

  # Merge aircraft states with airport data based on hex ID (resolution 5)
  arr_dep_apt <- df |>
    inner_join(airports_df, by = setNames(hex_id_col, 'hex_id_5'),  relationship = "many-to-many") |>
    mutate(time = as.POSIXct(time), # Convert the 'time' column to POSIXct datetime format
           segment_status = '') # Initialize the 'segment_status' column with an empty string

  # For each group, identify start and end points based on time

  arr_dep_apt <- arr_dep_apt |>
    group_by(!!sym(track_id_col), ident) |>
    mutate(segment_status = case_when(
      row_number() == 1 ~ 'start',
      row_number() == n() ~ 'end',
      TRUE ~ as.character(segment_status)
    )) |>
    ungroup()

  # Filter to only include 'start' or 'end'
  filtered_df <- arr_dep_apt |>
    filter(segment_status %in% c('start', 'end'))

  # Separate DataFrames for 'start' and 'end'
  start_df <- filtered_df |>
    filter(segment_status == 'start') |>
    select(-segment_status) |>
    rename_with(~paste('start', ., sep = '_'), -c(!!sym(track_id_col), 'ident'))

  end_df <- filtered_df |>
    filter(segment_status == 'end') |>
    select(-segment_status) |>
    rename_with(~paste('end', ., sep = '_'), -c(!!sym(track_id_col), 'ident'))

  # Merge the start and end DataFrames on 'id' and 'ident'
  apt_detections_df <- start_df |>
    full_join(end_df, by = c(track_id_col, 'ident'), na_matches = "never")

  # Select core columns and adjust as needed to match specific requirements
  core_columns <- c(track_id_col, 'ident', 'start_time', 'start_statevector_id', 'end_time', 'end_statevector_id')
  apt_detections_df <- apt_detections_df |>
    select(all_of(core_columns))

  return(apt_detections_df)
}


#' Identify Runways from Low Altitude Trajectories
#'
#' Analyzes aircraft trajectories at low altitudes to identify corresponding runways.
#' This function merges low altitude trajectory data with airport detection data to
#' filter and match trajectories with runways based on temporal and spatial criteria.
#'
#' @param apt_detections_df Dataframe of airport detections.
#' @param df_f_low_alt Dataframe of flight trajectories with low altitude.
#' @return A dataframe with information on matched runways for each trajectory.
#' @importFrom dplyr mutate select inner_join filter arrange group_by summarise ungroup unnest
#' @importFrom lubridate ymd_hms
#' @export
identify_runways_from_low_trajectories <- function(apt_detections_df, df_f_low_alt) {

  # Step 0: Creation of an ID & renaming cols
  apt_detections_df <- apt_detections_df |>
    mutate(apt_detection_id = paste(id, row_number() - 1, sep = "_")) |>
    select(id, apt_det_ident = ident, apt_det_start_time = start_time, apt_det_end_time = end_time, apt_det_id = apt_detection_id)

  # Step 1: Convert datetime columns to datetime format if they are not already
  apt_detections_df <- apt_detections_df |>
    mutate(apt_det_start_time = ymd_hms(apt_det_start_time),
           apt_det_end_time = ymd_hms(apt_det_end_time))

  # Step 2: Merge the data frames on 'id'
  merged_df <- inner_join(df_f_low_alt, apt_detections_df, by = "id", relationship = "many-to-many")

  # Step 3: Filter rows where 'time' is between 'apt_det_start_time' and 'apt_det_end_time'
  result_df <- merged_df |>
    filter(time >= apt_det_start_time & time <= apt_det_end_time)

  # Define the match_runways_to_hex function
  match_runways_to_hex <- function(df_low, .apt_det_id, .apt) {
    df_single <- df_low |>
      filter(apt_det_id == .apt_det_id) |>
      select(apt_det_id, id, time, lat, lon, hex_id_11, baroaltitude_fl)

    core_cols_rwy <- c("id", "airport_ref", "airport_ident", "gate_id", "hex_id", "gate_id_nr", "le_ident", "he_ident")
    tryCatch({
      df_rwys <- load_dataset(name = paste0(.apt, ".parquet"), datatype = "runway_hex") |>
        select(all_of(core_cols_rwy)) |>
        rename(c('id_apt' = 'id'))
    }, error = function(e) {
      print(paste0('Warning: Due to limited data in OurAirports, airport [', .apt,'] does not have the runway config. No matching for this airport.'))
      df_rwys = tibble(id = numeric(),
                    airport_ident = character(),
                    gate_id = character(),
                    hex_id = character(),
                    le_ident = character(),
                    he_ident = character(),
                    min_time = as.POSIXct(character()),
                    max_time = as.POSIXct(character()))
    })

    df_hex_rwy <- left_join(df_single, df_rwys, by = c("hex_id_11" = "hex_id"))

    result <- df_hex_rwy |>
      group_by(apt_det_id, id, airport_ident, gate_id, le_ident, he_ident) |>
      summarise(min_time = min(time), max_time = max(time), .groups = "drop") |>
      arrange(min_time) |>
      filter(!is.na(airport_ident)) |>
      select(-c(apt_det_id, id,))

    return(result)
  }

  # Step 5: Apply match_runways_to_hex for each row in apt_detections_df
  apt_detections_df <- apt_detections_df |>
    rowwise() |>
    mutate(matched_runways = list(match_runways_to_hex(result_df, apt_det_id, apt_det_ident))) |>
    ungroup()

  # Unnest the matched_runways column to expand the nested data frames into a single frame
  final_df <- apt_detections_df |>
    unnest(matched_runways)

  final_df <- final_df |>
    arrange(id, apt_det_id, airport_ident, min_time) |>
    group_by(id, apt_det_id, airport_ident, le_ident, he_ident) |>
    mutate(min_time = ymd_hms(min_time),
           max_time = ymd_hms(max_time),
           time_diff = as.numeric(difftime(min_time, lag(min_time), units = "mins")),
           time_diff = if_else(is.na(time_diff), 0, time_diff),
           new_det_id = cumsum(ifelse(is.na(time_diff) | time_diff > 40, 1, 0)),
           rwy_det_id = paste(apt_det_id, new_det_id, sep = "_")) |>
    ungroup() |>
    select(c('id', 'apt_det_id', 'rwy_det_id', 'airport_ident', 'gate_id', 'le_ident', 'he_ident', 'min_time', 'max_time'))

  return(final_df)
}


#' Manipulate Dataframe and Determine Arrival or Departure
#'
#' Processes trajectory data to determine whether each trajectory corresponds to an arrival or a departure.
#' This involves data manipulation steps including cleaning, merging, and filtering.
#'
#' @param df Dataframe containing detailed trajectory and runway intersection data.
#' @return A list containing two dataframes: one with aggregated trajectory information and
#' another indicating whether each trajectory corresponds to an arrival or departure.
#' @importFrom dplyr mutate filter select rename group_by summarise ungroup full_join
#' @importFrom lubridate difftime
#' @importFrom purrr map map_chr map_dbl
#' @export
manipulate_df_and_determine_arrival_departure <- function(df) {
  # Copying the DataFrame
  result <- df

  # Cleaning gate_id and adding new columns
  result <- result |>
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
    ) |>
    select(-gate_info)

  # Renaming 'id' to 'id_x'
  result <- result |>
    rename(id_x = id)

  # Identifying min and max distances
  result <- result |>
    group_by(id_x, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident) |>
    mutate(
      min_gate_distance = min(gate_distance_from_rwy_nm),
      max_gate_distance = max(gate_distance_from_rwy_nm)
    ) |>
    ungroup()

  # Filtering for min and max distance rows
  result_min <- result |>
    filter(gate_distance_from_rwy_nm == min_gate_distance)

  result_max <- result |>
    filter(gate_distance_from_rwy_nm == max_gate_distance)

  # Preparing the final DataFrame
  cols_of_interest <- c("id_x", "apt_det_id", "rwy_det_id", "airport_ident", "le_ident", "he_ident", "min_gate_distance", "min_time", "max_time", "max_gate_distance", "gate_distance_from_rwy_nm")

  result_min <- result_min |>
    select(all_of(cols_of_interest)) |>
    rename(
      time_entry_min_distance = min_time,
      min_gate_distance_from_rwy_nm = gate_distance_from_rwy_nm
    )

  result_max <- result_max |>
    select(all_of(cols_of_interest)) |>
    rename(
      time_entry_max_distance = min_time,
      max_gate_distance_from_rwy_nm = gate_distance_from_rwy_nm
    )

  det <- full_join(result_min, result_max, by = c("id_x", "apt_det_id", "rwy_det_id", "airport_ident", "le_ident", "he_ident"))

  # Compute the status based on time difference
  det <- det |>
    mutate(
      time_since_minimum_distance_s = as.numeric(difftime(time_entry_min_distance, time_entry_max_distance, units = "secs")),
      status = case_when(
        time_since_minimum_distance_s > 0 ~ "arrival",
        TRUE ~ "departure"
      )
    ) |>
    select(id_x, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident, status)

  # Aggregating data
  result <- result |>
    group_by(id_x, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident, gate_type) |>
    summarise(
      entry_time_approach_area = min(min_time),
      exit_time_approach_area = max(max_time),
      intersected_subsections = n(),
      minimal_distance_runway = min(gate_distance_from_rwy_nm),
      maximal_distance_runway = max(gate_distance_from_rwy_nm)
    ) |>
    ungroup()

  list(result = result, det = det)
}

#' Score and Apply Heuristics for Runway Detection
#'
#' Applies a set of heuristics to score and identify the most likely runway used by aircraft.
#' This function processes trajectory data, applying various criteria to score each possible
#' runway detection, then determines the most likely runway based on the highest score.
#'
#' @param df Dataframe containing detailed trajectory and gate intersection data.
#' @param det Dataframe containing arrival or departure status for each trajectory.
#' @return A dataframe with runways determined based on scoring heuristics, including information
#' on the likely runway, score, and status (arrival/departure/undetermined) for each detection.
#' @importFrom dplyr select filter mutate left_join replace_na group_by slice_max ungroup summarise
#' @export
score_and_apply_heuristics <- function(df, det) {
  # Define columns to use for runway results
  rwy_result_cols <- c('id_x', 'apt_det_id', 'rwy_det_id', 'airport_ident', 'le_ident', 'he_ident')

  # Filter and select for runway_hexagons gate type, then mark runway_detected
  rwy_result <- df |>
    select(all_of(c(rwy_result_cols, 'gate_type'))) |>
    filter(gate_type == 'runway_hexagons') |>
    select(all_of(rwy_result_cols)) |>
    mutate(runway_detected = TRUE)

  # Merge with original dataframe to mark runway_detected
  result <- df |>
    left_join(rwy_result, by = rwy_result_cols) |>
    mutate(runway_detected = replace_na(runway_detected, FALSE))

  # Exclude 'runway_hexagons' from gate_type for further analysis
  result <- result |>
    filter(gate_type != 'runway_hexagons') |>
    mutate(high_number_intersections = intersected_subsections > 5,
           low_minimal_distance = minimal_distance_runway < 5,
           touched_closest_segment_to_rw = minimal_distance_runway == 1,
           touched_second_closest_segment_to_rw = minimal_distance_runway <= 2)

  # Weights for scoring
  weights <- c(approach_detected = 0.3, runway_detected = 2, high_number_intersections = 1,
               low_minimal_distance = 1, touched_closest_segment_to_rw = 1.5,
               touched_second_closest_segment_to_rw = 0.75)

  max_score <- sum(weights)

  # Calculate score
  result <- result |>
    mutate(score = (1 * weights['approach_detected'] +
                      as.numeric(runway_detected) * weights['runway_detected'] +
                      as.numeric(high_number_intersections) * weights['high_number_intersections'] +
                      as.numeric(low_minimal_distance) * weights['low_minimal_distance'] +
                      as.numeric(touched_closest_segment_to_rw) * weights['touched_closest_segment_to_rw'] +
                      as.numeric(touched_second_closest_segment_to_rw) * weights['touched_second_closest_segment_to_rw']) / max_score * 100)

  # Merge with 'det' dataframe and handle missing 'status'
  result <- result |>
    left_join(det, by = c('id_x', 'apt_det_id', 'rwy_det_id', 'airport_ident', 'le_ident', 'he_ident')) |>
    mutate(status = replace_na(status, 'undetermined'),
           rwy = paste(le_ident, he_ident, sep = "/"))

  # Determine winners based on max score
  rwy_winner <- result |>
    group_by(id_x, apt_det_id, rwy_det_id, airport_ident) |>
    slice_max(order_by = score, with_ties = FALSE) |>
    ungroup() |>
    mutate(score = as.character(score)) |>
    group_by(id_x, apt_det_id, rwy_det_id, airport_ident) |>
    summarise(le_ident = first(le_ident), he_ident = first(he_ident),
              rwy = first(rwy), score = first(score), status = first(status), .groups = 'drop') |>
    rename(id = id_x, likely_rwy = rwy, likely_rwy_score = score, likely_rwy_status = status)

  # Mark winners in the original dataframe
  rwy_winner_flag <- select(rwy_winner, id, apt_det_id, rwy_det_id, airport_ident, le_ident, he_ident)
  rwy_winner_flag$winner <- TRUE

  result <- result |>
    rename(id = id_x) |>
    left_join(rwy_winner_flag, by = c('id', 'apt_det_id', 'rwy_det_id', 'airport_ident', 'le_ident', 'he_ident')) |>
    mutate(winner = replace_na(winner, FALSE))

  # Identify losers for alternative runways
  rwy_losers <- result |>
    filter(!winner) |>
    mutate(score = as.character(score)) |>
    group_by(id, apt_det_id, rwy_det_id, airport_ident) |>
    summarise(le_ident = first(le_ident), he_ident = first(he_ident),
              rwy = first(rwy), score = first(score), status = first(status), .groups = 'drop') |>
    rename(potential_other_rwys = rwy, potential_other_rwy_scores = score, potential_other_rwy_status = status) |>
    select(id, apt_det_id, airport_ident, potential_other_rwys, potential_other_rwy_scores, potential_other_rwy_status)

  # Merge winners and losers
  rwy_determined <- left_join(rwy_winner, rwy_losers, by = c('id', 'apt_det_id', 'airport_ident'))

  return(rwy_determined)
}

#' Identify Runways Used by Aircraft from Trajectory Data
#'
#' Integrates multiple steps to identify runways used by aircraft based on trajectory data.
#' This function processes aircraft state vectors, adding statevector IDs, converting barometric
#' altitudes, filtering low altitude state vectors, identifying potential airports, and determining
#' runway use through low trajectory analysis and scoring heuristics.
#'
#' @param df Dataframe containing aircraft state vectors.
#' @param track_id_col Name of the column containing track identifiers.
#' @param longitude_col Name of the column containing longitude values.
#' @param latitude_col Name of the column containing latitude values.
#' @param baroaltitude_col Name of the column containing barometric altitude values.
#' @return A list containing dataframes: one with scored runway detections and another with detailed
#' runway detections data before scoring.
#' @importFrom purrr list
#' @export
identify_runways <-
  function(df,
           track_id_col = 'id',
           longitude_col = 'lon',
           latitude_col = 'lat',
           baroaltitude_col = 'baroaltitude'){

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
        apt_types = c('large_airport', 'medium_airport')
      )

    rwy_detections_df <-
      identify_runways_from_low_trajectories(apt_detections_df, df_f_low_alt)

    res <- manipulate_df_and_determine_arrival_departure(rwy_detections_df)

    det = res$det
    rwy_detections_df = res$result

    scored_rwy_detections_df <- score_and_apply_heuristics(rwy_detections_df, det)

    return(list(scored_rwy_detections_df = scored_rwy_detections_df, rwy_detections_df = rwy_detections_df))
  }
