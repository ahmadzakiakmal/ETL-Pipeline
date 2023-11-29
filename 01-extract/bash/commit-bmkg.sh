cd ~/Repositories/ETL-Pipeline/raw-bmkg

# Extract the current date and time
mydate=$(date +"%Y-%m-%d")
mytime=$(date +"%H:%M:%S")

# Construct the commit message
commit_message="[Auto] BMKG Data $mydate-$mytime"
echo "$commit_message"

# Use the commit_message variable without percent signs
git add *
git commit -m "$commit_message"