package ns

import (
	"io/ioutil"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/pkg/errors"
	"github.com/zclconf/go-cty/cty"
)

// ResolverConfig is a configuration of a single resolver.
type ResolverConfig struct {
	// Name of the command used to execute the resolver.
	Name string `hcl:"resolver,label"`

	// Options is a list of additional options to execute the resolver.
	Options cty.Value `hcl:"options,optional"`

	Preload bool `hcl:"preload,optional"`

	// Processes is a number of processes to start for handling requests.
	// Default value is 1.
	Processes int `hcl:"processes,optional"`

	// MaxIdle is a maximum client requests waiting for processing.
	// Default value is 1024.
	MaxIdle int `hcl:"maxidle,optional"`
}

// ServerConfig is a configuration for a single server instance.
type ServerConfig struct {
	// ResolverDirectory sets the root directory for resolvers, when
	// specified server searches the executable in the given path,
	// otherwise a current directory will be used.
	ResolverDirectory string `hcl:"resolver_directory,optional"`

	// Listen defines an IP address and port used to listen to
	//
	// Examples:
	//
	// 	listen = ":5353"
	// 	listen = "0.0.0.0:53"
	Listen string `hcl:"listen,optional"`

	// Proto specifies the protocol that will be used for communication
	// with remotes queriers.
	//
	// Examples:
	//
	// 	proto = "udp"
	//	proto = "tcp"
	Proto string `hcl:"proto,optional"`

	// Maximum number of concurrent connection, zero is no limit.
	//
	// Example:
	//
	//	max_conns = 512
	MaxConns int `hcl:"max_conns,optional"`

	// RequestTimeout is an optional maximum time for processing each request
	// by a single resolver.
	RequestTimeout string `hcl:"request_timeout,optional"`

	// Resolvers is a sequence of resolution plugins that are sequentially
	// polled in order to retrieve a response on the processing request.
	//
	// Each server starts resolvers isolated from other servers.
	Resolvers []ResolverConfig `hcl:"resolver,block"`
}

// Config is a structure that holds configurations for the whole DNS server.
type Config struct {
	// ServerStoragePath is the location of the database file to keep
	// zones and associated records.
	ServerStoragePath string `hcl:"server_storage_path,optional"`

	// ServerShutdownTimeout specifies a timeout for graceful shutdown
	// of a server. When timeout expires, the termination will be enforced.
	ServerShutdownTimeout string `hcl:"server_shutdown_timeout,optional"`

	// Servers is a list of servers to process user requests.
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
