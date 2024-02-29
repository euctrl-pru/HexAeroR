# library(devtools)
# library(lubridate)
# library(arrow)
#
# library(HexAeroR)
#
# ensure_data_available()
#
# df2 <- read_parquet('data/2023-08-01-13.parquet')
#
# df2 <- df2 |>  mutate(
#   id = paste0(icao24, '_',callsign, '_', date(time))) %>%
#   filter(!is.na(lat), !is.na(lon), !is.na(baroaltitude))
#
# # ids <- df2 |> pull(id) |> unique()
# #
# # df2 <- df2 |> filter(id %in% ids[1:30])
#
# res <- identify_runways(
#   df2,
#   track_id_col = "id",
#   longitude_col = "lon",
#   latitude_col = "lat",
#   baroaltitude_col = "baroaltitude"
# )
#
# res$scored_rwy_detections_df
#
# # # #
# # # #
# # # #
# # # # match_runways_to_hex <- function(df_low, .apt_det_id, .apt) {
# # # #   df_single <- df_low |>
# # # #     filter(apt_det_id == .apt_det_id) |>
# # # #     select(apt_det_id, id, time, lat, lon, hex_id_11, baroaltitude_fl)
# # # #
# # # #   core_cols_rwy <- c("id", "airport_ref", "airport_ident", "gate_id", "hex_id", "gate_id_nr", "le_ident", "he_ident")
# # # #
# # # #   # Initialize df_rwys before the tryCatch block
# # # #   df_rwys <-
# # # #
# # # #     df_rwys <- tryCatch({
# # # #       load_dataset(name = paste0(.apt, ".parquet"), datatype = "runway_hex") %>%
# # # #         select(all_of(core_cols_rwy)) %>%
# # # #         rename(id_apt = id)
# # # #     }, error = function(e) {
# # # #       print(paste0('Warning: Due to limited data in OurAirports, airport [', .apt, '] does not have the runway config. No matching for this airport.'))
# # # #       tibble(id_apt = numeric(),
# # # #              airport_ident = character(),
# # # #              gate_id = character(),
# # # #              hex_id = character(),
# # # #              le_ident = character(),
# # # #              he_ident = character(),
# # # #              min_time = as.POSIXct(character()),
# # # #              max_time = as.POSIXct(character()))
# # # #     })
# # # #   df_hex_rwy <- left_join(df_single, df_rwys, by = c("hex_id_11" = "hex_id"))
# # # #
# # # #   if (length(df_hex_rwy$id_apt) == 0){
# # # #     return(tibble())
# # # #   } else {
# # # #     result <- df_hex_rwy |>
# # # #       group_by(apt_det_id, id, airport_ident, gate_id, le_ident, he_ident) |>
# # # #       summarise(min_time = min(time, na.rm = TRUE), max_time = max(time, na.rm = TRUE), .groups = "drop") |>
# # # #       arrange(min_time) |>
# # # #       filter(!is.na(airport_ident)) |>
# # # #       select(-c(apt_det_id, id,))
# # # #
# # # #     return(result)
# # # #   }
# # # # }
# # # #
# # # # apt_detections_df <- read_parquet('airport_detections_df.parquet')
# # # # df_f_low_alt <- read_parquet('df_f_low_alt.parquet')
# # # # merged_df <- read_parquet("merged_df.parquet")
# # # # result_df  <- read_parquet('result_df.parquet')
# # # #
# # # # library(tidyverse)
# # # # df_low <- result_df
# # # # .apt_det_id <- '4bc843_NA_2023-08-01_0'
# # # # .apt <- 'LFPB'
# # # #
# # # # match_runways_to_hex(df_low, .apt_det_id, .apt)
# # # #
# # # # apt_detections_df <- apt_detections_df |>
# # # #   rowwise() |>
# # # #   mutate(matched_runways = list(match_runways_to_hex(result_df, apt_det_id, apt_det_ident))) |>
# # # #   ungroup()
# # # #
# # # # final_df <- apt_detections_df |>
# # # #   unnest(matched_runways)
# # # #
# # # # # #
# # # # # # ids <- df2 |> pull(id)
# # # # # #
# # # # # # classified_df <- df2 |>
# # # # # #   mutate(detected_rwy = id %in% unique(res$scored_rwy_detections_df$id)) |>
# # # # # #   filter(id %in% ids[100:130])
# # # # # #
# # # # # #
# # # # # # library(leaflet)
# # # # # # library(dplyr)
# # # # # # library(magrittr)
# # # # # #
# # # # # # unique_ids <- unique(classified_df$id)
# # # # # # colors <- colorFactor(palette = "viridis", domain = unique_ids)
# # # # # #
# # # # # # # Create the Leaflet map
# # # # # # unique_ids <- unique(classified_df$detected_rwy)
# # # # # # colors <- colorFactor(palette = "viridis", domain = unique_ids)
# # # # # #
# # # # # # # Create the Leaflet map
# # # # # # leaflet(classified_df) %>%
# # # # # #   addTiles() %>%
# # # # # #   addCircleMarkers(
# # # # # #     ~lon, ~lat,
# # # # # #     color = ~colors(detected_rwy),
# # # # # #     #popup = ~paste("Callsign:", callsign, "<br>Velocity:", velocity, "m/s"),
# # # # # #     radius = 1
# # # # # #   ) %>%
# # # # # #   addLegend("bottomright",
# # # # # #             pal = colors,
# # # # # #             values = ~detected_rwy,
# # # # # #             title = "detected_rwy",
# # # # # #             opacity = 1
# # # # # #   )
# # # # # #
# # # # # # library(htmlwidgets)
# # # # # #
# # # # # # saveWidget(map, 'EGLL_rwy_detection.html')
# # # # # #
# # # # # #
# # # # # # df |> filter(id == '43ea7c_MSHRM_2023-08-01') |> View()
# # # # # #
# # # # # #
# # # # # # df |> group_by(id) |> summarize(count = n()) |> arrange(count) |> View()
# # # # # #
# # # # # #
# # # # # # df |> ggplot(mapping = aes(x=as.numeric(likely_rwy_score))) + geom_histogram(bins=10)
# # # # # #
# # # # # #
# # # # # # df |> pull(likely_rwy_score) |> table()
