function gen_sub() {
    local result="${1}"
    case $OSTYPE in
        msys|win32) result="//XX=x${result}"
    esac
    echo "$result"
}
SUB=`gen_sub "/C=US/CN=foo"`
openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 -subj $SUB -keyout key.pem -out cert.pem
