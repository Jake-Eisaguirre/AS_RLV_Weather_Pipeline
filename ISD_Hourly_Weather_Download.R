# github download: remotes::install_github("ropensci/rnoaa")
# Packages
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

librarian::shelf(rnoaa, tidyverse, lubridate, rvest, here)

airports_dim <- read_csv(here("airport_codes", "AIRPORT_DIM.csv")) %>% 
  rename(code = AIRPORT_CODE)

airports <- read_csv(here("airport_codes", "airports.csv"))

dest <- inner_join(airports_dim, airports, by = "code") %>% 
  select(code, icao, CITY_NAME, COUNTRY, name)

# Integrated Surface Database (ISD) compiled by NCDC within NOAA
#https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database

# Get a list of available ISD stations
stations <- isd_stations() %>% 
  filter(!usaf == "999999",
         !wban == "99999") %>%
  filter(icao %in% dest$icao)

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
    station_data <- read_csv(file)
    
    # Bind to the combined data frame
    combined_data <- bind_rows(combined_data, station_data)
    
    message(paste("Added data from:", file))
  }, error = function(e) {
    message(paste("Error reading file:", file, "-", e$message))
  })
}

