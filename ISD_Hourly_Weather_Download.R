# github download: remotes::install_github("ropensci/rnoaa")
# Packages
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

librarian::shelf(rnoaa, tidyverse, lubridate, rvest, here, lutz)

airports_dim <- read_csv(here("airport_codes", "AIRPORT_DIM.csv")) %>% 
  rename(code = AIRPORT_CODE)

airports <- read_csv(here("airport_codes", "airports.csv"))

dest <- inner_join(airports_dim, airports, by = "code") %>% 
  select(code, icao, CITY_NAME, COUNTRY, name)

# Integrated Surface Database (ISD) compiled by NCDC within NOAA
#https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database
#


# Get a list of available ISD stations
stations <- isd_stations() %>% 
  filter(!usaf == "999999",
         !wban == "99999") %>%
  filter(icao %in% dest$icao) %>% 
  mutate(timezone = tz_lookup_coords(lat, lon, method = "fast"))


# Nested loop for station and year combination
for (i in 1:nrow(stations)) {
  for (year in c(2023, 2024)) {
    usaf_code <- stations$usaf[i]
    wban_code <- stations$wban[i]
    station_icao <- stations$icao[i]
    
    # Fetch data using the ISD function for each station and year
    tryCatch({
      station_data <- isd(usaf = usaf_code, wban = wban_code, year = year)
      
      # Skip if no data
      if (nrow(station_data) == 0) {
        message(paste("No data for station:", stations$station_name[i], "in year:", year))
        next
      }
      
      required_columns <- c("AA1_depth", "GA1_coverage_code")
      missing_columns <- setdiff(required_columns, colnames(station_data))
      for (col in missing_columns) {
        station_data[[col]] <- NA
      }
      
      
      # Process the data
      station_data <- station_data %>%
        rename(precip_mm = AA1_depth, sky_cover_percent = GA1_coverage_code) %>%
        mutate(
          station_name = stations$station_name[i],
          year = year,
          icao = station_icao,
          date = as.Date(date, format = "%Y%m%d"),
          time = as.character(time),
          usaf_station = as.character(usaf_station),
          wban_station = as.character(wban_station),
          latitude = as.numeric(latitude),
          longitude = as.numeric(longitude),
          elevation = as.numeric(elevation),
          temperature = as.numeric(temperature) / 10,
          temperature_dewpoint = as.numeric(temperature_dewpoint) / 10,
          wind_speed = as.numeric(wind_speed) / 10,
          wind_direction = as.numeric(wind_direction),
          ceiling_height = as.numeric(ceiling_height) / 10,
          visibility_distance = as.numeric(visibility_distance) / 10,
          air_pressure = as.numeric(air_pressure) / 10,
          precip_mm = as.numeric(precip_mm) / 10,
          sky_cover_percent = as.numeric(sky_cover_percent)
        ) %>%
        mutate(
          temperature = if_else(temperature == 999.9, NA_real_, temperature),
          temperature_dewpoint = if_else(temperature_dewpoint == 999.9, NA_real_, temperature_dewpoint),
          wind_speed = if_else(wind_speed == 999.9, NA_real_, wind_speed),
          wind_direction = if_else(wind_direction == 999, NA_real_, wind_direction),
          ceiling_height = if_else(ceiling_height == 9999.9, NA_real_, ceiling_height),
          visibility_distance = if_else(visibility_distance == 99999.9, NA_real_, visibility_distance),
          air_pressure = if_else(air_pressure == 9999.9, NA_real_, air_pressure),
          precip_mm = if_else(precip_mm == 999.9, NA_real_, precip_mm),
          sky_cover_percent = if_else(sky_cover_percent == 99, NA_real_, sky_cover_percent)
        ) %>%
        select(date, time, icao, temperature, temperature_dewpoint, wind_speed, ceiling_height, 
               visibility_distance, air_pressure, precip_mm, sky_cover_percent) %>% 
        distinct()
      
      # Save the data to a CSV file
      file_name <- here("station_data", paste0(station_icao, "_", year, ".csv"))
      write_csv(station_data, file_name)
      message(paste("Saved data for station:", station_icao, "in year:", year))
      
    }, error = function(e) {
      message(paste("Error fetching data for", stations$station_name[i], "in", year, "-", e$message))
    })
  }
  print(paste("Processed station:", stations$station_name[i], "-", i, "of", nrow(stations)))
}


# Get a list of all CSV files in the folder
file_list <- list.files(here("station_data"), pattern = "\\.csv$", full.names = TRUE)

# Initialize an empty data frame
combined_data <- data.frame()

# Loop through each file and bind the data
for (file in file_list) {
  tryCatch({
    # Read the current file
    station_data <- read_csv(file, col_types = cols())
    
    combined_data_d <- station_data %>%
      mutate(
        time = str_pad(time, width = 4, pad = "0"),  # Pad time to ensure 4 digits
        time = paste0(substr(time, 1, 2), ":", substr(time, 3, 4)),  # Format time as HH:MM
        hour = as.numeric(substr(time, 1, 2))  # Extract the hour as a numeric value
      ) %>% 
      group_by(date, icao, hour) %>% 
      reframe(
        temperature_c = round(mean(temperature), 2),
        dewpoint_c = round(mean(temperature_dewpoint), 2),
        wind_speed_ms = round(mean(wind_speed), 2),
        ceiling_height_m = round(mean(ceiling_height), 2),
        visibility_m = round(mean(visibility_distance), 2),
        air_pressure_ = round(mean(air_pressure), 2),
        precip_mm = round(mean(precip_mm), 2),
        sky_cover_percent = round(mean(sky_cover_percent), 2)
      ) %>%
      # Format hour as HH:00
      mutate(hour = sprintf("%02d:00", hour)) %>% 
      rename(time_utc = hour)
    
    
    # Bind to the combined data frame
    combined_data <- bind_rows(combined_data, combined_data_d)
    
    message(paste("Added data from:", file))
  }, error = function(e) {
    message(paste("Error reading file:", file, "-", e$message))
  })
}

t_zone <- stations %>% 
  select(icao, timezone)

tz_off_set <- tz_list() %>% 
  select(tz_name, is_dst, utc_offset_h) %>% 
  filter(tz_name %in% unique(t_zone$timezone))

# Define DST cutoff dates for 2023 and 2024
dst_start_2023 <- as.Date("2023-03-12")
dst_end_2023 <- as.Date("2023-11-05")
dst_start_2024 <- as.Date("2024-03-10")
dst_end_2024 <- as.Date("2024-11-03")

combined_data_tz <-  combined_data %>%
  mutate(
    date = as.Date(date),  # Ensure date column is in Date format
    is_dst = if_else((date >= dst_start_2023 & date <= dst_end_2023) | 
                     (date >= dst_start_2024 & date <= dst_end_2024),
                     TRUE,
                     FALSE)) %>% 
  left_join(t_zone, by = c("icao")) %>% 
  left_join(tz_off_set, by = c("timezone" = "tz_name", "is_dst")) %>% 
  mutate(utc_date_time = as.POSIXct(paste(date, time_utc), format = "%Y-%m-%d %H:%M", tz = "UTC"),
         local_date_time = utc_date_time + hours(utc_offset_h)) %>% 
  relocate(local_date_time, .before = date) %>% 
  select(!c(date, time_utc))


write_csv(combined_data, here("data", "hourly", "hourly_weather_data.csv"))
                
# # Define paths
# default_cache_path <- tools::file_path_as_absolute("C:/Users/025883/AppData/Local/R/cache/R/rnoaa/noaa_isd")
# desired_cache_path <- here("raw_cached_data")
# 
# 
# # Get a list of all files in the default cache folder
# cached_files <- list.files(default_cache_path, full.names = TRUE)
# 
# # Move each file to the desired cache folder
# for (file in cached_files) {
#   new_file_location <- file.path(desired_cache_path, basename(file))
#   file.rename(file, new_file_location)
#   message(paste("Moved file to:", new_file_location))
# }            
#                 
                