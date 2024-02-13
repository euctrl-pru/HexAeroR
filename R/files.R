library(httr)
library(fs)
library(progress)
library(zip)
library(tidyverse)

download_and_extract_zip <- function(url, extract_to) {
  if (!dir_exists(extract_to)) {
    message(paste("Creating directory", extract_to, "..."))
    dir_create(extract_to, recurse = TRUE)
  }

  # Extract the basename without query parameters
  file_name <- basename(url)
  local_zip_path <- str_replace(file.path(extract_to, file_name), '\\?download=1', '')

  message(paste("Downloading data from", url, "..."))
  response <- GET(url, progress())

  if (http_status(response)$category == "Success") {
    # Use writeBin to save the content
    writeBin(content(response, "raw"), con = local_zip_path)

    message(paste("Extracting data to", extract_to, "..."))
    unzip(local_zip_path, exdir = extract_to)
    file_delete(local_zip_path)
    message("Download and extraction complete.")
  } else {
    stop("Error in downloading the file: HTTP status code ", response$status_code)
  }
}


setup_data_directory <- function(base_dir) {
  urls <- c(
    "https://zenodo.org/records/10651018/files/airport_hex.zip?download=1",
    "https://zenodo.org/records/10651018/files/runway_hex.zip?download=1",
    "https://zenodo.org/records/10651018/files/test_data.zip?download=1"
  )
  walk(urls, ~download_and_extract_zip(.x, base_dir))
}


ensure_data_available <- function() {
  data_dir <- file.path(str_replace(dirname(sys.frame(1)$ofile), '/R', ''), 'data')
  required_files <- c('airport_hex.parquet', 'runway_hex.parquet', 'test_data.parquet')

  if (!all(map_lgl(required_files, ~dir_exists(file.path(data_dir, .x))))) {
    user_response <- readline(prompt = "Required metadata parquet files not found. Download (~200MB) and setup now? [y/n]: ")
    if (tolower(user_response) == 'y') {
      message("Downloading required data files...")
      setup_data_directory(data_dir)
    } else {
      message("Data download skipped. The package requires data files to function properly.")
    }
  }
}

# Uncomment the line below to call the function, noting this will prompt the user when sourced or run.
#ensure_data_available()

