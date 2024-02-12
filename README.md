<img src="https://raw.githubusercontent.com/euctrl-pru/HexAeroR/main/assets/hexaeror_logo.png" alt="HexAeroR logo" style="border: 1px solid black" align="right" width="300"/>

# HexAeroR [under construction]

<!-- badges: start -->
<!-- badges: end -->

HexAeroR is a EUROCONTROL R package designed for aviation professionals and data analysts. It allows for the determination of used airports, runways, taxiways, and stands based on available (ADS-B) flight trajectory coordinates. This tool aims to enhance aviation data analysis, facilitating the extraction of milestones for performance analysis.

## Features

-   **Airport Detection**: Identifies airports involved in a flight's trajectory.
-   **Runway Utilization**: Determines which runways are used during takeoff and landing.


## Installation

You can install the development version of HexAeroR from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("euctrl-pru/HexAeroR")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(HexAeroR)
## basic example code
```

This will prompt you to download the geospatial metadata from Zenodo necessary to run the package. It will only cache once. The parquet datasets are available here: <https://zenodo.org/records/10651018>.


``` python
# Load trajectory data
df <- load_dataset(name = "trajectories.parquet", datatype = "test_data")

# Create unique ID for each trajectory
df$id <- paste(df$icao24, df$callsign, as.Date(df$time), sep = "-")

# Identify runways

list(scored_rwy_detections_df, rwy_detections_df) <- identify_runways(df)
```

## Usage

### Visualizing Methodology

``` r

# Load approach hex dataset for an airport (e.g., EGLL)
egll <- load_dataset(name = "EGLL.parquet", datatype = "runway_hex")

# Visualize approach cones using leaflet for interactive maps
library(leaflet)

map_viz <- leaflet(egll) %>%
  addTiles() %>%
  addPolygons(color = ~border_color, fillOpacity = .7, fillColor = ~color_map_name) %>%
  setView(lng = mean(df$lon), lat = mean(df$lat), zoom = 13) %>%
  addPopups(~lon, ~lat, ~tooltip_columns)

# Print the map
print(map_viz)
```

![Runway detection](https://raw.githubusercontent.com/euctrl-pru/HexAeroR/main/assets/egll_departure.png "Departure of a flight of runway 09R/27L at EGLL as detected by HexAeroPy.")

Download the HTML here: [egll_departure.html](https://github.com/euctrl-pru/HexAeroR/blob/main/assets/egll_departure.html)

## Development Roadmap

-   **[pending implementation] Taxiway Analysis**: Analyzes taxi routes for efficiency and optimization.
-   **[pending implementation] Stand Identification**: Identifies aircraft stands, enhancing ground operation analysis.

## Contributing

We welcome contributions to HexAeroR! Feel free to submit pull requests, report issues, or request features.

## License

HexAeroPy is licensed under the MIT License - see the [LICENSE](https://github.com/euctrl-pru/HexAeroPy/blob/main/LICENSE) file for details.

## Credits and Acknowledgments

Special thanks to [EUROCONTROL](https://www.eurocontrol.int/) and the [Performance Review Commission (PRC)](https://ansperformance.eu/about/prc/).

## Contact Information

For support or queries, please contact us at [pru-support\@eurocontrol.int](mailto:pru-support@eurocontrol.int).
