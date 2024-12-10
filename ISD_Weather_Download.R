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

# Initialize an empty data frame to store results
all_weather_data <- data.frame()

# Initialize counter for completed airports
completed_airports <- 0


replace_inf_with_na <- function(x) {
  ifelse(is.infinite(x), NA, x)
}

# Nested loop for station and year combination
for (i in 1:nrow(stations)) {
  for (year in c(2023, 2024)) {
    usaf_code <- stations$usaf[i]
    wban_code <- stations$wban[i]
    
    # Fetch data using the ISD function for each station and year
    tryCatch({
      station_data <- isd(usaf = usaf_code, wban = wban_code, year = year)
      
      # Debug: Check if data is fetched
      if (nrow(station_data) == 0) {
        message(paste("No data for station:", stations$station_name[i], "in year:", year))
        next
      }
      
      print(paste("Fetched data for:", stations$station_name[i], "in", year))
      
      # Add missing columns with NA if not present
      required_columns <- c("AA1_depth", "GA1_coverage_code")
      missing_columns <- setdiff(required_columns, colnames(station_data))
      for (col in missing_columns) {
        station_data[[col]] <- NA
      }
      
      # Process the data: wrangle, join, and select relevant features
      station_data_d <- station_data %>%
        rename(precip_mm = AA1_depth,
               sky_cover_percent = GA1_coverage_code ) %>%
        mutate(
          station_name = stations$station_name[i],
          year = year,
          icao = stations$icao[i],
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
        select(date, usaf_station, wban_station, year, icao, temperature, temperature_dewpoint,
               wind_speed, ceiling_height, visibility_distance, air_pressure,
               precip_mm, sky_cover_percent)
      
      # Summarize data: min and max per day per station
      station_data_summary <- station_data_d %>%
        group_by(date, usaf_station, wban_station, icao) %>%
        summarize(
          min_temperature = replace_inf_with_na(min(temperature, na.rm = TRUE)),
          max_temperature = replace_inf_with_na(max(temperature, na.rm = TRUE)),
          min_dewpoint = replace_inf_with_na(min(temperature_dewpoint, na.rm = TRUE)),
          max_dewpoint = replace_inf_with_na(max(temperature_dewpoint, na.rm = TRUE)),
          min_wind_speed = replace_inf_with_na(min(wind_speed, na.rm = TRUE)),
          max_wind_speed = replace_inf_with_na(max(wind_speed, na.rm = TRUE)),
          min_ceiling_height = replace_inf_with_na(min(ceiling_height, na.rm = TRUE)),
          max_ceiling_height = replace_inf_with_na(max(ceiling_height, na.rm = TRUE)),
          min_visibility_distance = replace_inf_with_na(min(visibility_distance, na.rm = TRUE)),
          max_visibility_distance = replace_inf_with_na(max(visibility_distance, na.rm = TRUE)),
          min_air_pressure = replace_inf_with_na(min(air_pressure, na.rm = TRUE)),
          max_air_pressure = replace_inf_with_na(max(air_pressure, na.rm = TRUE)),
          min_precip_mm = replace_inf_with_na(min(precip_mm, na.rm = TRUE)),
          max_precip_mm = replace_inf_with_na(max(precip_mm, na.rm = TRUE)),
          min_sky_cover_percent = replace_inf_with_na(min(sky_cover_percent, na.rm = TRUE)),
          max_sky_cover_percent = replace_inf_with_na(max(sky_cover_percent, na.rm = TRUE)),
          .groups = "drop"
        )
      
      # Append summarized data to the combined summary data frame
      all_weather_data <- rbind(all_weather_data, station_data_summary)
    }, error = function(e) {
      message(paste("Error fetching data for", stations$station_name[i], "in", year, "-", e$message))
    })
  }
  # Increment and print the count of completed airports
  completed_airports <- completed_airports + 1
  print(paste("Completed airports:", completed_airports, "of", nrow(stations)))
}

# Final check
if (nrow(all_weather_data) == 0) {
  message("No weather data was summarized. Please verify data sources or processing logic.")
} else {
  print("Data summarization completed successfully.")
}

airport_code <- dest %>% 
  select(code, icao)

all_weather_data <- all_weather_data %>% 
  left_join(airport_code, by = "icao") %>% 
  select(!c(usaf_station, wban_station)) %>% 
  relocate(code, .after = date)

# 136 stations contain data, could in the future use the lat long to geo_join to look for other nearby weather stations
write_csv(all_weather_data, here("data", "daily_min_max", "all_weather_data.csv"))


