package hectocorn

import (
	"encoding/json"
	"os"
)

// DecodeConfig decodes the configuration of the plugin from environment
// variable to the v. Method returns nil when configuration is missing
// in environment.
func DecodeConfig(v interface{}) error {
	b := os.Getenv("hectodns.options")
	if b == "" {
		return nil
	}

	return json.Unmarshal([]byte(b), v)
}
