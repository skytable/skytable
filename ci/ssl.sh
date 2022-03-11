function gen_sub() {
    local result="${1}"
    case $OSTYPE in
        msys|win32) result="//XX=x${result}"
    esac
    echo "$result"
}

openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 -subj $(gen_sub '/C=US/CN=example.com') -keyout key.pem -out cert.pem
