library(httr)
library(fs)
library(progress)
library(zip)
library(tidyverse)
library(utils)

#' Download and Extract ZIP File from URL
#'
#' This function downloads a ZIP file from a specified URL and extracts it into a target directory.
#' It creates the target directory if it does not exist. The function handles the download progress
#' display and checks for successful download status.
#'
#' @param url Character string specifying the URL of the ZIP file to download.
#' @param extract_to Character string specifying the path to the directory where the ZIP file should be extracted.
#' @return None
#' @import httr fs progress
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
    utils::unzip(local_zip_path, exdir = extract_to, setTimes = FALSE)
    file_delete(local_zip_path)
    message("Download and extraction complete.")
  } else {
    stop("Error in downloading the file: HTTP status code ", response$status_code)
  }
}

#' Setup Data Directory with Required Files
#'
#' This function sets up a data directory by downloading and extracting necessary ZIP files.
#' It processes a list of URLs pointing to ZIP files, downloading and extracting each into the specified base directory.
#'
#' @param base_dir Character string specifying the base directory into which the data will be extracted.
#' @return None
#' @importFrom purrr walk
setup_data_directory <- function(base_dir) {
  urls <- c(
    "https://zenodo.org/records/10651018/files/airport_hex.zip?download=1",
    "https://zenodo.org/records/10651018/files/runway_hex.zip?download=1",
    "https://zenodo.org/records/10651018/files/test_data.zip?download=1"
  )
  walk(urls, ~download_and_extract_zip(.x, base_dir))
}

#' Ensure Required Data Files are Available
#'
#' This function checks if required data files are available in a specified directory.
#' If not, it prompts the user to download and set up the necessary files.
#' The function is intended to be run before the main analysis to ensure all needed data is present.
#'
#' @return None
#' @importFrom purrr map_lgl
#' @export
ensure_data_available <- function() {
  data_dir <- str_replace(system.file("NAMESPACE", package = "HexAeroR"), '/NAMESPACE', '/data/')

  if (dir.exists(data_dir)==FALSE){
    dir.create(data_dir)
  }

  required_dirs <- c('airport_hex', 'runway_hex', 'test_data')

  if (!all(map_lgl(required_dirs, ~dir_exists(file.path(data_dir, .x))))) {
    user_response <- readline(prompt = "Required metadata parquet files not found. Download (~200MB) and setup now? [y/n]: ")
    if (tolower(user_response) == 'y') {
      message("Downloading required data files...")
      setup_data_directory(data_dir)
    } else {
      message("Data download skipped. The package requires data files to function properly.")
    }
  } else {
    print('Data packages are loaded correctly.')
  }
}

# Uncomment the line below to call the function, noting this will prompt the user when sourced or run.
#ensure_data_available()

