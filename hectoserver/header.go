package hectoserver

import (
	"net"
	"strconv"
	"strings"
)

type ForwardedHeader struct {
	Proto string
	Host  string
	By    net.TCPAddr
	For   []net.TCPAddr
}

// ParseForwarded parses a Forwarded header and returns all passed
// directives within a forwarded map.
func ParseForwarded(header string) (fw ForwardedHeader, err error) {
	var (
		raw_dirs = strings.Split(header, ";")
		dirs     = make([]string, 0, len(raw_dirs))
	)

	// Each directive can represent a list of values, like it happens for
	// a directive "for":
	//
	// 	Forwarded: for=192.168.0.1,for=192.168.0.2;by=192.168.0.254
	for _, directive := range raw_dirs {
		dirs = append(dirs, strings.Split(directive, ",")...)
	}

	for _, directive := range dirs {
		// Each directive reprsents a key-value pair: by=1.2.3.4
		kv := strings.SplitN(strings.Trim(directive, " "), "=", 2)
		if len(kv) != 2 {
			return ForwardedHeader{}, err
		}

		key := strings.ToLower(kv[0])
		val := kv[1]

		// For directives that does not support lists (proto, host, by),
		// simply override the value when a new identifier appears.
		switch key {
		case "proto":
			fw.Proto = val
		case "host":
			fw.Host = val
		case "by":
			ip, port, err := parseHostPort(val)
			if err != nil {
				return ForwardedHeader{}, err
			}
			fw.By = net.TCPAddr{IP: ip, Port: port}
		case "for":
			ip, port, err := parseHostPort(val)
			if err != nil {
				return ForwardedHeader{}, err
			}
			fw.For = append(fw.For, net.TCPAddr{IP: ip, Port: port})
		}
	}

	return
}

func parseHostPort(s string) (ip net.IP, port int, err error) {
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = strings.TrimLeft(s, `"`)
		s = strings.TrimRight(s, `"`)
	}

	portIndex := strings.LastIndex(s, ":")

	if portIndex > 0 {
		i, err := strconv.ParseInt(s[portIndex+1:], 10, 0)
		if err != nil {
			return nil, 0, err
		}

		port = int(i)
		s = s[:portIndex]
	}

	if strings.HasPrefix(s, `[`) && strings.HasSuffix(s, `]`) {
		s = strings.TrimLeft(s, `[`)
		s = strings.TrimRight(s, `]`)
	}

	ip = net.ParseIP(s)
	if ip == nil {
		return ip, port, &net.ParseError{Type: "IP address", Text: s}
	}

	return
}
