# CODE FOR RUNNING LSH AND MSM AND EVALUATING MSM

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

# ------------------------------------------------------------------------------
# --------------------- LOCALITY SENSITIVE HASHING (LSH) -----------------------
# ------------------------------------------------------------------------------

# Possible values of r (rows per band) that divide 150 exactly
# possible_r <- c(2, 3, 5, 6, 10, 15, 25, 50, 75)

# Define the number of bands (b) and rows per band (r)
m <- nrow(signature_matrix)
r <- 5 # Number of rows per band
b <- m/r # Number of bands
t <- (1/b)^(1/r)

# Function to hash a band to a bucket
hash_band <- function(band) {
  digest(paste(band, collapse = ""), algo = "xxhash32")
}

# Initialize an empty list to hold the hash buckets
hash_buckets <- vector("list", b)

# Initialize a list to store candidate pairs
candidate_pairs <- list()

# Perform LSH on the signature matrix
for (i in 1:b) {
  # Extract the rows for the current band
  start_row <- (i - 1) * r + 1
  end_row <- min(i * r, m)
  band <- signature_matrix[start_row:end_row, ]
  
  # Initialize a hash table for the current band
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
  
  # Add the hash table to the list of hash buckets
  hash_buckets[[i]] <- hash_table
  
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

# -------------------------------------------------------------------------------
# ---------- DEFINE FUNCTION FOR SIMILARITY CALCULATIONS -----------------------
# -------------------------------------------------------------------------------

# Functions
calcSim <- function(str1, str2, q = 3) {
  # Ensure inputs are character and handle NA values
  if (is.na(str1) || is.na(str2)) {
    return(0)
  }
  
  # Convert strings to lowercase and remove punctuation/whitespace
  str1 <- tolower(gsub("[[:punct:]\\s]+", "", str1))
  str2 <- tolower(gsub("[[:punct:]\\s]+", "", str2))
  
  # Calculate q-gram distance
  distance <- stringdist::stringdist(str1, str2, method = "qgram", q = q)
  
  # Number of q-grams in each string
  n1 <- nchar(str1) - q + 1
  n2 <- nchar(str2) - q + 1
  
  # Avoid division by zero
  if (n1 <= 0 || n2 <= 0) {
    return(0)
  }
  
  # Convert distance to similarity
  similarity <- 1 - (distance / (n1 + n2))
  
  # Ensure similarity is within bounds [0, 1]
  return(max(0, similarity))
}

sameShop <- function(p1, p2) {
  return(p1$webshop == p2$webshop)
}

diffBrand <- function(p1, p2) {
  if (!is.na(p1$brand) && !is.na(p2$brand))  { 
    return(p1$brand != p2$brand)
  } 
  return(FALSE)
}

extract_model_words <- function(value) {
  words <- unlist(strsplit(tolower(value), "\\W+"))
  words <- words[words != ""]
  return(unique(words))
}

# -------------------------------------------------------------------------------
# ---------- CALCULATE SIMILARITY AND FIND POTENTIAL DUPLICATES ----------------
# -------------------------------------------------------------------------------

# Define the parameter grid
gamma_values <- c(0.5, 0.5, 0.7, 0.7, 0.7)
epsilon_values <- c(0.9, 0.9, 0.8, 0.9, 0.8)
mu_values <- c(0.3, 0.5, 0.3, 0.5, 0.5)

# Number of rows needed for the grid
num_rows <- length(gamma_values)

# Create an empty matrix with the correct number of rows and columns
results_matrix <- matrix(nrow = num_rows, ncol = 7)
colnames(results_matrix) <- c("gamma", "epsilon", "mu", "pair_completeness", 
                              "pair_quality", "num_candidates", "F1")

# Loop through the grid and populate the matrix
for (row_index in 1:num_rows) {
  gamma <- gamma_values[row_index]
  epsilon <- epsilon_values[row_index]
  mu <- mu_values[row_index]
  # Initialize dissimilarity matrix
  dissimilarity <- matrix(Inf, nrow = nrow(product_info), ncol = nrow(product_info))
  
  # Loop through all candidate pairs
  for (pair_index in 1:nrow(candidate_pairs_df)) {
    
    if (pair_index %% 100 == 0) {
      cat(pair_index, "of", nrow(candidate_pairs_df), "\n")
    }
    
    p1 <- candidate_pairs_df[pair_index, 1]
    p2 <- candidate_pairs_df[pair_index, 2]
    
    product1 <- product_info[p1, ]
    product2 <- product_info[p2, ]
    
    # Check same shop or different brand
    if (sameShop(product1, product2)) {
      next
    }
    if (diffBrand(product1, product2)) {
      next
    }
    
    # Extract Key-Value Pairs
    features_p1 <- names(split_data[[p1]])[grepl("^featuresMap\\.", names(split_data[[p1]]))]
    kvp_p1 <- data.frame(
      Feature = gsub("^featuresMap\\.", "", features_p1),
      Value = unlist(split_data[[p1]][features_p1]),
      stringsAsFactors = FALSE
    )
    kvp_p1 <- kvp_p1[!is.na(kvp_p1$Value), ]
    
    features_p2 <- names(split_data[[p2]])[grepl("^featuresMap\\.", names(split_data[[p2]]))]
    kvp_p2 <- data.frame(
      Feature = gsub("^featuresMap\\.", "", features_p2),
      Value = unlist(split_data[[p2]][features_p2]),
      stringsAsFactors = FALSE
    )
    kvp_p2 <- kvp_p2[!is.na(kvp_p2$Value), ]
    
    # Initialize similarity calculation
    sim <- 0
    avgSim <- 0
    m <- 0
    w <- 0
    
    # Compare key-value pairs
    unmatched_p1 <- kvp_p1
    unmatched_p2 <- kvp_p2
    
    for (i in 1:nrow(kvp_p1)) {
      for (j in 1:nrow(kvp_p2)) {
        keySim <- calcSim(kvp_p1$Feature[i], kvp_p2$Feature[j])
        if (keySim > gamma) {
          valueSim <- calcSim(kvp_p1$Value[i], kvp_p2$Value[j])
          weight <- keySim
          sim <- sim + weight * valueSim
          m <- m + 1
          w <- w + weight
          unmatched_p1 <- unmatched_p1[-i, ]
          unmatched_p2 <- unmatched_p2[-j, ]
        }
      }
    }
    
    if (w > 0) {
      avgSim <- sim / w
    }
    
    # Model Word Similarity
    mw_p1 <- unlist(map(unmatched_p1$Value, extract_model_words))
    mw_p2 <- unlist(map(unmatched_p2$Value, extract_model_words))
    mw_p1 <- unique(mw_p1)
    mw_p2 <- unique(mw_p2)
    
    mwPerc <- length(intersect(mw_p1, mw_p2)) / max(length(mw_p1), length(mw_p2))
    
    # Title Similarity (dummy placeholder, replace with actual TMWMSim function)
    titleSim <- calcSim(product1$title, product2$title)
    if (titleSim == -1) {
      theta1 <- m / min(nrow(kvp_p1), nrow(kvp_p2))
      theta2 <- 1 - theta1
      hSim <- theta1 * avgSim + theta2 * mwPerc
    } else {
      theta1 <- (1 - mu) * (m / min(nrow(kvp_p1), nrow(kvp_p2)))
      theta2 <- 1 - mu - theta1
      hSim <- theta1 * avgSim + theta2 * mwPerc + mu * titleSim
    }
    
    # Transform to dissimilarity
    dissimilarity[p1, p2] <- 1 - hSim
    dissimilarity[p2, p1] <- dissimilarity[p1, p2]
  }
  
  # Perform Hierarchical Clustering
  dissimilarity_dist <- as.dist(dissimilarity)
  hc_complete <- hclust(dissimilarity_dist, method = "complete")
  
  # Determine clusters
  clusters <- cutree(hc_complete, h = epsilon)
  
  clustered_products <- data.frame(
    productID = product_info$productID,
    cluster = clusters
  )
  
  potential_duplicates <- clustered_products %>%
    group_by(cluster) %>%
    summarise(indexes = list(which(clustered_products$cluster == cluster))) %>%
    ungroup() %>%
    pull(indexes) %>%
    lapply(function(x) if (length(x) > 1) as.data.frame(t(combn(x, 2))) else NULL) %>%
    Filter(Negate(is.null), .)
  
  potential_duplicate_pairs <- do.call(rbind, potential_duplicates)
  colnames(potential_duplicate_pairs) <- c("product1", "product2")
  
  # Evaluate performance
  evaluation_results_MSM <- evaluate_performance(potential_duplicate_pairs, golden_standard_pairs)
  
  results_matrix[row_index, ] <- c(gamma, epsilon, mu, 
                                   evaluation_results_MSM$pair_completeness, 
                                   evaluation_results_MSM$pair_quality, 
                                   evaluation_results_MSM$num_candidates, 
                                   evaluation_results_MSM$F1)
  
  
  # Print progress
  cat("gamma:", gamma, "epsilon:", epsilon, "mu:", mu, "\n")
  print(evaluation_results_MSM)
  
  row_index <- row_index + 1
}



# Convert to data frame for easier handling
results_df <- as.data.frame(results_matrix)

# Save results to a CSV file
write.csv(results_df, "C:/Users/pauli/Desktop/grid_results_r=5.csv", row.names = FALSE)

# Display the results
print(results_df)
