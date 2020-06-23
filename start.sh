echo "Unpacking data ..."
unzip data.zip

echo "Installing dependencies ..."
conda install -r requirements.txt

echo "Updating env with env vars from: $PWD/.env"
echo ""
cat "$PWD/.env"
export $(cat "$PWD/.env" | xargs);

