
# Load packages -----------------------------------------------------------
library(here)
library(readr)
library(fs)
library(dplyr)
library(janitor)
library(stringr)


# Load data ---------------------------------------------------------------

riders <- tribble(
  ~rider, ~horse, ~horse_id, ~final_time,
  "Aminda Ingulfson", "Joystick", "joystick", 425
)

jump_ids <- c("1", "2", "3", "4", "5a", "5b", "5c", "6a", "6b", "7", "8a", "8b",
              "8c", "9", "10a", "10b", "10c", "11", "12a", "12b", "13", "14a",
              "14b", "14c", "15", "16", "17a", "17b", "18a", "18b", "18c", "19",
              "20a", "20b", "21")

jumps_raw <- dir_ls(here("data-raw"), glob = "*jumps*") |>
  read_delim(
    delim = ";",
    id = "horse_id",
    na = c("", "NA", "-"),
    name_repair = make_clean_names
  )


strides_raw <- dir_ls(here("data-raw"), glob = "*strides*") |>
  read_delim(delim = ";", id = "horse_id", name_repair = make_clean_names)

# Clean data --------------------------------------------------------------

jump_clean <- jumps_raw |>
  mutate(
    horse_id = str_extract(basename(horse_id), "(.*)_(.*)_(.*)", group = 2),
    across(
      c(lateral_balance, straightness, longitudinal_balance),
      \(x) as.numeric(str_extract(x, "[\\-\\.0-9]*"))
    ),
    lateral_balance2 = lateral_balance,
  )

strides_clean <- strides_raw |>
  mutate(
    horse_id = str_extract(basename(horse_id), "(.*)_(.*)_(.*)", group = 2),
    across(
      c(duration, strike_power, speed, max_height, length, lateral_balance,
        straightness, longitudinal_balance),
      \(x) as.numeric(str_extract(x, "[\\-\\.0-9]*"))
    ),
    max_height = max_height / 100,
    across(
      c(strike_power, lateral_balance, straightness, longitudinal_balance),
      list(min = ~.x - 0.5, max = ~.x + 0.5)
    )
  )


# Calculate jump data -------------------------------------------------

# Add data from strides (for correct time)
jump_added_info <- jump_clean |>
  left_join(
    strides_clean,
    join_by(
      horse_id,
      stride_height_m == max_height,
      length_m == length,
      between(strike_power_g, strike_power_min, strike_power_max),
      avg_speed_m_min == speed,
      between(lateral_balance, lateral_balance_min, lateral_balance_max),
      between(straightness, straightness_min, straightness_max),
      between(longitudinal_balance, longitudinal_balance_min, longitudinal_balance_max),
    ),
    relationship = "many-to-one",
  )

# Check if all strides matched
missing_in_merged <- jump_added_info |>
  filter(type == "Stride") |>
  pull(energy) |>
  is.na() |>
  any()

if (missing_in_merged) {stop("Joining jumps and strides did not work")}

# Calculate time for jumps
jump_calc_time <- jump_added_info |>
  mutate(
    time_mm_ss = if_else(
      type == "Jump",
      lag(time_mm_ss) + (length_m / (avg_speed_m_min / 60)),
      time_mm_ss
    )
  )

# Add jump info
jump_filtered <- jump_calc_time |> filter(type == "Jump")

jump_filtered$jump_id <- rep(jump_ids, times = n_distinct(jump_calc_time$horse_id))

jump_selected <- jump_filtered |>
  select(
    horse_id,
    time_mm_ss,
    strike_power = strike_power_g,
    gait = type,
    speed = avg_speed_m_min,
    max_height = jump_height_m,
    length = length_m,
    lateral_balance = lateral_balance.x,
    straightness = straightness.x,
    max_take_off_angle,
    jump_id
  )

# Merge data --------------------------------------------------------------

full_data <- strides_clean |>
  bind_rows(jump_selected) |>
  arrange(horse_id, time_mm_ss)


# Calculations for full data ----------------------------------------------

full_filtered_data <- full_data |>
  group_by(horse_id) |>
  left_join(riders, by = join_by(horse_id)) |>
  mutate(
    time_mm_ss = time_mm_ss - min(time_mm_ss) + first(duration / 1000),
    total_length = cumsum(length)
  ) |>
  filter(time_mm_ss < final_time + 0.5) |>
  ungroup()

full_selected_data <- full_filtered_data |>
  select(
    rider,
    horse,
    time = time_mm_ss,
    distance = total_length,
    gait,
    speed,
    length,
    max_height,
    hand,
    lateral_balance,
    longitudinal_balance,
    straightness,
    jump_id
  )


# Save cleaned data -------------------------------------------------------

write_csv(full_selected_data, here("data/race_data.csv"))
