#!/bin/bash

# Usage: ./script.sh requirements.txt TOKEN_ENV_VAR
# Where TOKEN_ENV_VAR is the name of the environment variable containing the token

# Check if two arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <path to requirements.txt> <token env var name>"
    exit 1
fi

# Assign arguments to variables
requirements_file=$1
token_var_name=$2

# Retrieve the token from the environment variable
token=${!token_var_name}

# Check if the token is not empty
if [ -z "$token" ]; then
    echo "Token is empty. Make sure the environment variable $token_var_name is set."
    exit 1
fi

# Create a backup of the original requirements.txt
cp "$requirements_file" "${requirements_file}.bak"

# First replacement: Insert token using sed
sed -i.bak1 -e "/^full-metal-weaviate/s/https:\/\//https:\/\/${token}@/" "$requirements_file"

# Second replacement: Remove commit references using sed
# sed -i.bak2 -e "/^full-metal-monad\|^full-metal-weaviate/s/@[^@]*$//" "$requirements_file"

# sed -i.bak2 -e "/^full-metal-monad\|^full-metal-weaviate/s/@[^@]*$//" "$requirements_file"

sed -i.bak -E '/^full-metal-weaviate|^full-metal-monad/s/@[a-f0-9]*$//' "$requirements_file"

# sed -i.bak -e '/@/s/@[a-f0-9]*$//' "$requirements_file"

echo "Replacement complete. Original file backed up as ${requirements_file}.bak"



# #!/bin/bash

# # Usage: ./script.sh requirements.txt TOKEN_ENV_VAR
# # Where TOKEN_ENV_VAR is the name of the environment variable containing the token

# # Check if two arguments are provided
# if [ "$#" -ne 2 ]; then
#     echo "Usage: $0 <path to requirements.txt> <token env var name>"
#     exit 1
# fi

# # Assign arguments to variables
# requirements_file=$1
# token_var_name=$2

# # Retrieve the token from the environment variable
# token=${!token_var_name}

# # Check if the token is not empty
# if [ -z "$token" ]; then
#     echo "Token is empty. Make sure the environment variable $token_var_name is set."
#     exit 1
# fi

# # Create a backup of the original requirements.txt
# cp "$requirements_file" "${requirements_file}.bak"

# # Replace the line
# awk -v token="$token" '/^full-metal-weaviate/{sub(/https:\/\//, "https://" token "@"); print; next}1' "${requirements_file}.bak" > "$requirements_file"


# awk '{
#     if ($0 ~ /^full-metal-monad/ || $0 ~ /^full-metal-weaviate/) 
#         sub(/@[^@]*$/, "", $0);
#     print
# }' "$requirements_file" > "$requirements_file"


# # awk -v token="$token" '/^full-metal-weaviate/{print "full-metal-weaviate @ git+https://" token "@github.com/thedeepengine/full-metal-weaviate.git"; next}1' "${requirements_file}.bak" > "$requirements_file"

# echo "Replacement complete. Original file backed up as ${requirements_file}.bak"


