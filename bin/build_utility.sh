set -e 
DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT=$DIR/..
TARGET=$ROOT/modules/pipelin
ENV_UTILITY_FILE=$TARGET/utility.zip

pushd $TARGET

	ls -la
	
	if ls *.zip 1> /dev/null 2>&1; then
		echo "--------------- zip file existss, deleting ---------------"
		rm *.zip
	fi
	
	echo "--------------- zip dependencies ---------------"
	
	zip utility.zip *.py
	
	ls -la