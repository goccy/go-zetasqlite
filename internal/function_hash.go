package internal

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"

	"github.com/dgryski/go-farm"
)

func FARM_FINGERPRINT(v string) (Value, error) {
	return IntValue(farm.Fingerprint64([]byte(v))), nil
}

func MD5(v string) (Value, error) {
	sum := md5.Sum([]byte(v))
	encoded := base64.StdEncoding.EncodeToString(sum[:])
	return StringValue(encoded), nil
}

func SHA1(v string) (Value, error) {
	sum := sha1.Sum([]byte(v))
	encoded := base64.StdEncoding.EncodeToString(sum[:])
	return StringValue(encoded), nil
}

func SHA256(v string) (Value, error) {
	sum := sha256.Sum256([]byte(v))
	encoded := base64.StdEncoding.EncodeToString(sum[:])
	return StringValue(encoded), nil
}

func SHA512(v string) (Value, error) {
	sum := sha512.Sum512([]byte(v))
	encoded := base64.StdEncoding.EncodeToString(sum[:])
	return StringValue(encoded), nil
}
