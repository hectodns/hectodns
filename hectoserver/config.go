package hectoserver

import (
	"io/ioutil"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/pkg/errors"
)

// ResolverConfig is a configuration of a single resolver.
type ResolverConfig struct {
	// Name of the command used to execute the resolver.
	Name string `hcl:"resolve,label"`

	// Options is a list of additional options to execute the resolver.
	Options *[]string `hcl:"options,attr"`
}

// ServerConfig is a configuration for a single server instance.
type ServerConfig struct {
	// Listen defines an IP address and port used to listen to
	//
	// Examples:
	//
	// 	listen = ":5353"
	// 	listen = "0.0.0.0:53"
	Listen string `hcl:"listen"`

	// Proto specifies the protocol that will be used for communication
	// with remotes queriers.
	//
	// Examples:
	//
	// 	proto = "dns"
	//	proto = "http"
	Proto string `hcl:"proto,attr"`

	// Timeout is an optional maximum time for processing each request
	// by a single resolver.
	Timeout *int `hcl:"timeout,attr"`

	// Resolvers is a sequence of resolution plugins that are sequentially
	// polled in order to retrieve a response on the processing request.
	//
	// Each server starts resolvers isolated from other servers.
	Resolvers []ResolverConfig `hcl:"resolve,block"`
}

// Config is a structure that holds configurations for the whole DNS server.
type Config struct {
	Servers []ServerConfig `hcl:"server,block"`
}

// DecodeConfig opens the given filename and loads the configuration.
//
// Method returns an error, when file does not exist, configuration syntax
// is incorrect or the decoding failed.
func DecodeConfig(filename string) (*Config, error) {
	src, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	file, diags := hclsyntax.ParseConfig(src, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, errors.WithMessage(diags, "unable to parse configuration")
	}

	var config Config

	diags = gohcl.DecodeBody(file.Body, nil, &config)
	if diags.HasErrors() {
		return nil, errors.WithMessage(diags, "unable to decode configuration")
	}

	return &config, nil
}
