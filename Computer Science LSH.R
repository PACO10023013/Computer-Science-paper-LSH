# CODE FOR RUNNING LSH AND EVALUATING

# ------------------------------------------------------------------------------
# --------------------- LOAD LIBRARIES AND READ DATA --------------------------
# ------------------------------------------------------------------------------
# Load necessary libraries
library(jsonlite)
library(dplyr)
library(stringr)
library(purrr)
library(digest) 
library(stringdist)
library(cluster)
library(tidyverse)
library(fastcluster)
library(combinat)
library(data.table)

set.seed(123)
start_time <- Sys.time()

# Read the JSON data from the file
data <- fromJSON("C:/Users/pauli/Downloads/TVs-all-merged/TVs-all-merged.json", flatten = TRUE)
# data <- fromJSON("C:/Users/523644pc/Downloads/TVs-all-merged/TVs-all-merged.json", flatten = TRUE)

# Split each element of 'data' to ensure each data frame has only one row
split_data <- lapply(data, function(df) {
  split(df, seq_len(nrow(df)))
})

# Flatten the list of lists to a single-level list
split_data <- unlist(split_data, recursive = FALSE)


# Extract relevant info (ModelID, Title, Webshob)
product_info <- split_data %>%
  purrr::map_dfr(~ {
    tibble(
      productID = .x$modelID, 
      title = .x$title,       
      webshop = .x$shop     
    )
  })


# List of common brands
common_brands <- c(
  "Samsung", "LG", "Sony", "Panasonic", "Philips", "TCL", "Sharp", "Vizio", 
  "Hisense", "Sanyo", "JVC", "Magnavox", "RCA", "Hitachi", "Mitsubishi", "Zenith",
  "Toshiba", "Insignia", "Seiki", "Westinghouse", "Element", "Skyworth", "Vizio", 
  "Haier", "Onkyo", "Pioneer", "Bose", "Sharp", "Grundig", "Akai", "Vizio", 
  "Epson", "BenQ", "ViewSonic", "Acer", "Fujitsu", "Polaroid", "InFocus", "Sharp", 
  "Funai", "Xiaomi", "LeEco", "Loewe", "Bang & Olufsen", "Sennheiser", "Bose", 
  "Marantz", "Harman Kardon", "Harman", "Klipsch", "Bowers & Wilkins", "SuperSonic", 
  "Coby ", "Coby", "Naxa", "Sansui Signature", "SunBriteTV"
)
# Function to extract brand from title
extract_brand_from_title <- function(title, brand_list) {
  for (brand in brand_list) {
    if (str_detect(title, fixed(brand, ignore_case = TRUE))) {
      return(brand) 
    }
  }
  return(NA)  # If no brand is found, return NA
}

product_info$brand <- unlist(lapply(product_info$title, function(title) extract_brand_from_title(title, common_brands)))

# Function to preprocess text
preprocess_text <- function(text) {
  text <- tolower(text)  # Convert to lowercase
  text <- str_replace_all(text, "\\s+", " ")  # Remove extra spaces
  text <- str_replace_all(text, "[()]", "")  # Remove parentheses
  text <- str_replace_all(text, "(\\d+)\\s*[-\"'\\s]*(inch(es)?|in\\.|in|\"|''|-inch)", "\\1 inch")
  text <- str_replace_all(text, "(\\d+)\\s*[-\"'\\s]*(hertz|hz|hetrz)", "\\1 hz")
  text <- str_replace_all(text, "(\\d+)\\s*(diag\\.?|diagonal)", "\\1 diagonal")  # Normalize diagonal and class terms
  text <- str_replace_all(text, "\\s+(inch|hz)\\b", "\\1")
  text <- str_trim(text)
  return(text)
}


# Apply preprocessing
product_info <- product_info %>%
  mutate(title = preprocess_text(title))

# ------------------------------------------------------------------------------
# ----------------------- CREATE GOLDEN STANDARDS ------------------------------
# ------------------------------------------------------------------------------

# Create the golden standard dataframe of all true duplicate pairs
golden_standard_pairs <- expand.grid(product1 = 1:nrow(product_info), product2 = 1:nrow(product_info)) %>%
  filter(product1 < product2) %>%
  filter(product_info$productID[product1] == product_info$productID[product2])

golden_standard_sorted <- golden_standard_pairs %>%
  mutate(pair = paste0(pmin(product1, product2), "-", pmax(product1, product2)))

# ------------------------------------------------------------------------------
# --------------------- FUNCTION TO EVALUATE PERFORMANCE -----------------------
# ------------------------------------------------------------------------------
evaluate_performance <- function(candidate_pairs, ground_truth) {
  
  # Total duplicates in the ground truth
  total_duplicates <- nrow(ground_truth)
  
  candidate_pairs_sorted <- candidate_pairs%>%
    mutate(pair = paste0(pmin(product1, product2), "-", pmax(product1, product2)))
  
  # Identify which golden standard pairs are in the candidate pairs
  matched_pairs <- golden_standard_sorted$pair %in% candidate_pairs_sorted$pair
  
  # Calculate the number of matches
  num_matches <- sum(matched_pairs)
  
  # Pair completeness (Recall)
  pair_completeness <- round(num_matches / total_duplicates, 4)
  
  # Pair quality (Precision)
  pair_quality <- round(num_matches / nrow(candidate_pairs), 4)
  
  # F1 score (handling the case where both pair_quality and pair_completeness are zero)
  F1 <- (2 * pair_quality * pair_completeness) / (pair_quality + pair_completeness)
  
  # Return the results as a list
  return(list(
    pair_completeness = pair_completeness,
    pair_quality = pair_quality,
    F1 = F1,
    num_candidates = nrow(candidate_pairs) 
  ))
}
# ------------------------------------------------------------------------------
# --------------------- CREATE BINARY VECTOR FOR PRODUCTS ----------------------
# ------------------------------------------------------------------------------

# Function to check if a token is a model word
is_model_word <- function(token) {
  # Check if the token contains both letters and numbers or special characters
  contains_alphanumerical <- str_detect(token, "[a-zA-Z][0-9]|[0-9][a-zA-Z]")
  contains_numerical <- str_detect(token, "[0-9]")
  contains_special <- str_detect(token, "[^a-zA-Z0-9]")
  
  # A model word must match at least two of the three conditions
  sum(c(contains_alphanumerical, contains_numerical, contains_special)) >= 2
}

# Function to clean special characters in words
clean_special_chars <- function(word) {
  word <- str_replace_all(word, "^\\W+|\\W+$", "")
  word <- str_replace_all(word, "\\\\", "")
  word <- str_replace_all(word, "[()]", "")
  
  return(word)
}

# Extract model words from all titles and create a cleaned unique list
MW <- product_info %>%
  pull(title) %>%                  # Extract the 'title' column
  str_extract_all("\\S+") %>%      # Split titles into tokens (words)
  flatten_chr() %>%                # Flatten the list into a single character vector
  keep(is_model_word) %>%          # Keep only tokens that are model words
  map_chr(clean_special_chars) %>% # Clean special characters from each word
  unique()                         # Get unique model words


# Function to create binary vector for a product
create_binary_vector <- function(title, MW) {
  binary_vector <- sapply(MW, function(mw) ifelse(str_detect(title, mw), 1, 0))
  return(binary_vector)
}

# Create binary vectors for all products and transpose them
binary_vectors <- do.call(rbind, lapply(product_info$title, create_binary_vector, MW = MW))

# --------------------------------------------------------------------------------
# --------------------- MIN-HASHING ---------------------------------------------
# --------------------------------------------------------------------------------

# Function to create a random permutation
create_permutation <- function(length) {
  sample(1:length, length, replace = FALSE)
}

# Function to compute the minhash of a binary vector under a given permutation
compute_minhash <- function(binary_vector, permutation) {
  permuted_vector <- binary_vector[permutation]
  minhash <- which(permuted_vector == 1)[1]
  if (is.na(minhash)) minhash <- Inf
  return(minhash)
}

# Set number of minhashes (m)
# m <- nrow(binary_vectors)/ 2
m <- 150
p <- ncol(binary_vectors)
n <- nrow(product_info)

# Initialize an empty signature matrix
signature_matrix <- matrix(Inf, nrow = m, ncol = n)

# Compute minhash signatures
for (i in 1:m) {
  permutation <- create_permutation(p)
  for (j in 1:n) {
    signature_matrix[i, j] <- compute_minhash(binary_vectors[j,], permutation)
  }
}
  
  
# -----------------------------------------------------------------------------  
# --------------------- FUNCTION TO PERFORM LSH --------------------------------
# ------------------------------------------------------------------------------
# Function to create a random permutation
create_permutation <- function(length) {
  sample(1:length, length, replace = FALSE)
}

# Function to compute the minhash of a binary vector under a given permutation
compute_minhash <- function(binary_vector, permutation) {
  permuted_vector <- binary_vector[permutation]
  minhash <- which(permuted_vector == 1)[1]
  if (is.na(minhash)) minhash <- Inf
  return(minhash)
}


# Function to perform LSH
perform_lsh <- function(signature_matrix, r, b) {
  # Ensure that n = r * b
  n <- nrow(signature_matrix)
  if (r * b != n) {
    stop("The product of r and b must equal the number of rows in the signature matrix")
  }
  
  # Function to hash a band to a bucket
  hash_band <- function(band) {
    digest(paste(band, collapse = ""), algo = "xxhash32")
  }
  
  # Initialize a list to store candidate pairs
  candidate_pairs <- list()
  
  # Perform LSH on the signature matrix
  for (i in 1:b) {
    # Extract the rows for the current band
    start_row <- (i - 1) * r + 1
    end_row <- min(i * r, n)
    band <- signature_matrix[start_row:end_row, ]
    
    # Create a hash table for the current band
    hash_table <- list()
    
    # Hash each column (product vector) in the band
    for (j in 1:ncol(band)) {
      column <- as.numeric(band[, j])
      bucket <- hash_band(column)
      
      if (!is.null(hash_table[[bucket]])) {
        hash_table[[bucket]] <- c(hash_table[[bucket]], j)
      } else {
        hash_table[[bucket]] <- c(j)
      }
    }
    
    # Identify candidate pairs from the hash table
    for (bucket in names(hash_table)) {
      indices <- hash_table[[bucket]]
      if (length(indices) > 1) {
        pairs <- combn(indices, 2, simplify = FALSE)
        candidate_pairs <- c(candidate_pairs, pairs)
      }
    }
  }
  
  # Convert candidate pairs to a data frame
  candidate_pairs_df <- do.call(rbind, lapply(candidate_pairs, function(pair) {
    data.frame(product1 = pair[1], product2 = pair[2])
  }))
  
  # Remove duplicate pairs
  candidate_pairs_df <- unique(candidate_pairs_df)
  return(candidate_pairs_df)
}

# ------------------------------------------------------------------------------
# ----------------- PERFORM LSH FOR DIFFERENT PARAMETERS  ----------------------
# ------------------------------------------------------------------------------

# Set number of minhashes (m)
m <- 150
p <- ncol(binary_vectors)
n <- nrow(product_info)

# Possible values of r (rows per band) that divide 150 exactly
possible_r <- c(2, 3, 5, 6, 10, 15, 25, 50, 75)

# possible_r <- c(75, 25, 10)

# Calculate b (bands) and t (threshold) for each r
grid <- data.frame(
  Rows_per_Band_r = possible_r,
  Bands_b = m / possible_r,
  Threshold_t = (1 / (n / possible_r))^(1 / possible_r)
)

# Initialize results list
results <- list()

# Loop over each row in the grid
for (grid_values in 1:nrow(grid)) {
  start_time <- Sys.time()  # Start timing
  
  # Extract grid parameters
  r <- grid[grid_values, 1]
  b <- grid[grid_values, 2]
  t <- grid[grid_values, 3]
  
  # Perform LSH
  candidate_pairs_df <- perform_lsh(signature_matrix, r, b)

  # Evaluate the candidate pairs
  evaluation <- evaluate_performance(candidate_pairs_df, golden_standard_pairs)
  
  # Extract evaluation results
  evaluation_result <- list(
    threshold = t,
    r = r,
    b = b,
    num_candidates = nrow(row(candidate_pairs_df)),
    pair_completeness = evaluation$pair_completeness,
    pair_quality = evaluation$pair_quality,
    F1 = evaluation$F1
  )
  
  # Add to
  results[[length(results) + 1]] <- evaluation_result  
  end_time <- Sys.time()  
  run_time <- end_time - start_time
  print(run_time)
}

# Convert Results List to Data Frame
results_df <- do.call(rbind, lapply(results, function(evaluation_result) {
  data.frame(
    threshold = evaluation_result$threshold,
    r = evaluation_result$r,
    b = evaluation_result$b,
    num_candidates = evaluation_result$num_candidates,
    pair_completeness = evaluation_result$pair_completeness,
    pair_quality = evaluation_result$pair_quality,
    F1 = evaluation_result$F1,
    fraction = evaluation_result$num_candidates / (1624 * 1623)
  )
}))

# Print the final results
print(results_df)

# ------------------------------------------------------------------------------
# --------------------------- MAKE PLOTS ---------------------------------------
# ------------------------------------------------------------------------------

# Plot: Fraction of Comparisons vs Pair Completeness
ggplot(results_df, aes(x = fraction, y = pair_completeness)) +
  geom_line() +
  geom_point() +
  labs(title = "Fraction of Comparisons vs Pair Completeness",
       x = "Fraction of Comparisons",
       y = "Pair Completeness") +
  theme_minimal()

# Plot: Fraction of Comparisons vs Pair Quality
ggplot(results_df, aes(x = fraction, y = pair_quality)) +
  geom_line() +
  geom_point() +
  labs(title = "Fraction of Comparisons vs Pair Quality",
       x = "Fraction of Comparisons",
       y = "Pair Quality") +
  theme_minimal()

# Plot: Fraction of Comparisons vs F1 Score
ggplot(results_df, aes(x = fraction, y = F1)) +
  geom_line() +
  geom_point() +
  labs(title = "Fraction of Comparisons vs F1 Score",
       x = "Fraction of Comparisons",
       y = "F1 Score") +
  theme_minimal()

