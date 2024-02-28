# library(HexAeroR)
# library(lubridate)
#
# df <- HexAeroR::load_dataset('trajectories.parquet', 'test_data')
#
# df <- df |> mutate(
#   id = paste0(icao24, '_',callsign, '_', date(time))
# )
#
# res <- HexAeroR::identify_runways(df)
#
# res$rwy_detections_df |> View()
#
# res$scored_rwy_detections_df |> View()
