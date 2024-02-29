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
# res <- identify_runways(
#   df2,
#   track_id_col = "id",
#   longitude_col = "lon",
#   latitude_col = "lat",
#   baroaltitude_col = "baroaltitude"
# )
#
# ids <- df2 |> pull(id)
#
# classified_df <- df2 |>
#   mutate(detected_rwy = id %in% unique(res$scored_rwy_detections_df$id)) |>
#   filter(id %in% ids[100:130])
#
#
# library(leaflet)
# library(dplyr)
# library(magrittr)
#
# unique_ids <- unique(classified_df$id)
# colors <- colorFactor(palette = "viridis", domain = unique_ids)
#
# # Create the Leaflet map
# unique_ids <- unique(classified_df$detected_rwy)
# colors <- colorFactor(palette = "viridis", domain = unique_ids)
#
# # Create the Leaflet map
# leaflet(classified_df) %>%
#   addTiles() %>%
#   addCircleMarkers(
#     ~lon, ~lat,
#     color = ~colors(detected_rwy),
#     #popup = ~paste("Callsign:", callsign, "<br>Velocity:", velocity, "m/s"),
#     radius = 1
#   ) %>%
#   addLegend("bottomright",
#             pal = colors,
#             values = ~detected_rwy,
#             title = "detected_rwy",
#             opacity = 1
#   )
#
# library(htmlwidgets)
#
# saveWidget(map, 'EGLL_rwy_detection.html')
#
#
# df |> filter(id == '43ea7c_MSHRM_2023-08-01') |> View()
#
#
# df |> group_by(id) |> summarize(count = n()) |> arrange(count) |> View()
#
#
# df |> ggplot(mapping = aes(x=as.numeric(likely_rwy_score))) + geom_histogram(bins=10)
#
#
# df |> pull(likely_rwy_score) |> table()
