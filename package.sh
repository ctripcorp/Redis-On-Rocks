SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR=${SCRIPT_DIR}
SRC_DIR="${BUILD_DIR}/src"
REPACK_DIR="${BUILD_DIR}/repack"

PACK_TYPE=unknown
REDIS=redis
ROR=ror

if [[ $# != 1 || ($1 != "$REDIS" && $1 != "$ROR") ]] ; then echo "$0 $REDIS|$ROR"; exit 1; fi
if [[ $1 == $REDIS ]]; then PACK_TYPE=$REDIS; else PACK_TYPE=$ROR; fi

XREDIS_VERSION=$(cat ${SCRIPT_DIR}/src/ctrip.h| grep -w XREDIS_VERSION | awk -F \" '{print $2}')
SWAP_VERSION=$(cat ${SCRIPT_DIR}/src/version.h| grep -w SWAP_VERSION | awk -F \" '{print $2}')
PKG_COMPILE_COUNT="0"

if [[ $PACK_TYPE == $REDIS ]]; then
    XREDIS_NAME=xredis
    PKG_VERSION="$XREDIS_VERSION-$PKG_COMPILE_COUNT"
else
    XREDIS_NAME=xredis-ror
    PKG_VERSION="$XREDIS_VERSION-$SWAP_VERSION-$PKG_COMPILE_COUNT"
fi

OSRELEASE=""
OSBITS=`uname -p`

ARCH=$(echo `uname -s` | awk '{print toupper($1)}')
if [[ "$ARCH" == "LINUX" ]]; then
    os_release_file=/etc/os-release
    if [[ -s ${os_release_file} ]]; then
        . ${os_release_file}
        OSRELEASE="${ID}${VERSION_ID}"
    else
		echo "/etc/os-release file not found!"
		exit 1
    fi
else
    echo "The system $ARCH does not supported!"
    exit 1
fi

RED='\033[0;31m'
NC='\033[0m' # No Color
cecho() {
    local text="$RED[== $1 ==]$NC" 
    printf "$text\n"
}

# build & install
XREDIS_TAR_NAME=${XREDIS_NAME}"-"${PKG_VERSION}"-"${OSRELEASE}"-"${OSBITS}
XREDIS_TAR_BALL=${XREDIS_TAR_NAME}.tar.gz
XREDIS_INSTALL_DIR=${REPACK_DIR}/${XREDIS_TAR_NAME}

cecho "Build & install xredis to ${XREDIS_INSTALL_DIR}"

cd ${BUILD_DIR} && make distclean

if [[ $PACK_TYPE == $REDIS ]]; then
    cd ${BUILD_DIR} && make -j 32 >/dev/null
else
    cd ${BUILD_DIR} && make SWAP=1 -j 32 >/dev/null
fi

mkdir -p ${XREDIS_INSTALL_DIR}

cp ${SRC_DIR}/{redis-server,redis-benchmark,redis-cli,redis-check-rdb,redis-check-aof,../redis.conf} ${XREDIS_INSTALL_DIR}

if [[ $PACK_TYPE == $REDIS ]]; then
    cp ${SRC_DIR}/../redis.conf ${XREDIS_INSTALL_DIR}
else
    cp ${SRC_DIR}/../ror.conf ${XREDIS_INSTALL_DIR}
fi

#  package
cecho "Package ${XREDIS_TAR_BALL}"
cd ${REPACK_DIR} && tar -czvf ${XREDIS_TAR_BALL} ${XREDIS_TAR_NAME}

