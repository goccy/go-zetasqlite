package internal

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
)

func NET_HOST(v string) (Value, error) {
	parsed := parse_url(v)
	if parsed == nil {
		return nil, nil
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return nil, nil
	}
	if strings.HasPrefix(parsed.Host, "[") {
		return StringValue("[" + hostname + "]"), nil
	}
	return StringValue(hostname), nil
}

func NET_IP_FROM_STRING(v string) (Value, error) {
	ip := parse_ip(v)
	if ip == nil {
		return nil, fmt.Errorf("NET.IP_FROM_STRING: invalid ip address %T", v)
	}
	return BytesValue(ip), nil
}

func NET_IP_NET_MASK(output, prefix int64) (Value, error) {
	result := net.CIDRMask(int(prefix), int(output)*8)
	if output != 4 && output != 16 {
		return nil, fmt.Errorf("NET.IP_NET_MASK: the first argument must be either 4 or 16")
	}
	if prefix < 0 || prefix > output*8 {
		return nil, fmt.Errorf("NET.IP_NET_MASK: the second argument must be in the range from 0 to %d", output*8)
	}
	return BytesValue(result), nil
}

func NET_IP_TO_STRING(v []byte) (Value, error) {
	ip, ok := netip.AddrFromSlice(v)
	if !ok {
		return nil, fmt.Errorf("NET.IP_TO_STRING: invalid byte array")
	}
	return StringValue(ip.String()), nil
}

func NET_IP_TRUNC(v []byte, length int64) (Value, error) {
	if len(v) != 4 && len(v) != 16 {
		return nil, fmt.Errorf("NET.IP_TRUNC: length of the first argument must be either 4 or 16")
	}
	if length < 0 || int(length) > len(v)*8 {
		return nil, fmt.Errorf("NET.IP_TRUNC: length must be in the range from 0 to %d", len(v)*8)
	}
	ip := net.IP(v)
	mask := net.CIDRMask(int(length), len(v)*8)
	return BytesValue(ip.Mask(mask)), nil
}

func NET_IPV4_FROM_INT64(v int64) (Value, error) {
	ip := make([]byte, 4)
	binary.BigEndian.PutUint32(ip, uint32(v))
	return BytesValue(ip), nil
}

func NET_IPV4_TO_INT64(v []byte) (Value, error) {
	if len(v) != 4 {
		return nil, fmt.Errorf("NET.IPV4_TO_INT64: length of bytes array must be 4")
	}
	return IntValue(binary.BigEndian.Uint32(v)), nil
}

func NET_PUBLIC_SUFFIX(v string) (Value, error) {
	parsed := parse_url(v)
	if parsed == nil {
		return nil, nil
	}
	host := parsed.Hostname()
	suffix := public_suffix(host)
	if suffix == "" {
		return nil, nil
	}
	return StringValue(suffix), nil
}

func NET_REG_DOMAIN(v string) (Value, error) {
	parsed := parse_url(v)
	if parsed == nil {
		return nil, nil
	}
	host := parsed.Hostname()
	split_host := strings.Split(host, ".")
	suffix := public_suffix(host)
	split_suffix := strings.Split(suffix, ".")
	if host == "" || suffix == "" || len(split_host) <= len(split_suffix) {
		return nil, nil
	}
	return StringValue(strings.Join(split_host[len(split_host)-len(split_suffix)-1:], ".")), nil
}

func NET_SAFE_IP_FROM_STRING(v string) (Value, error) {
	ip := parse_ip(v)
	if ip == nil {
		return nil, nil
	}
	return BytesValue(ip), nil
}

func parse_url(v string) *url.URL {
	parsed, err := url.Parse(v)
	if err != nil {
		return nil
	}
	if parsed.Host == "" {
		parsed, err = url.Parse("//" + strings.TrimSpace(v))
		if err != nil {
			return nil
		}
	}
	return parsed
}

func public_suffix(host string) string {
	reg, _ := regexp.Compile(`[^.]\.{2,}[^.]`)
	if reg.MatchString(host) {
		return ""
	}
	split_host := strings.Split(host, ".")
	encoded, err := idna.ToASCII(strings.ToLower(host))
	if err != nil {
		return ""
	}
	suffix, icann := publicsuffix.PublicSuffix(encoded)
	if !icann {
		return ""
	}
	split_suffix := strings.Split(suffix, ".")
	return strings.Join(split_host[len(split_host)-len(split_suffix):], ".")
}

func parse_ip(v string) []byte {
	ip, err := netip.ParseAddr(v)
	if err != nil {
		return nil
	}
	return ip.AsSlice()
}
