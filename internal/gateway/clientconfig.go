package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// ServerAlias is the name of the gateway's entry in a Perkeep client
// config's "servers".
const ServerAlias = "keepsync"

// DefaultClientConfigPath is where stock Perkeep tools run by this user
// look for their client config: $CAMLI_CONFIG_DIR/client-config.json, or
// client-config.json in the user's perkeep config directory. It reads the
// calling process's environment, so call it from the user's CLI process.
func DefaultClientConfigPath() (string, error) {
	if d := os.Getenv("CAMLI_CONFIG_DIR"); d != "" {
		return filepath.Join(d, "client-config.json"), nil
	}
	var dir string
	switch {
	case runtime.GOOS == "windows":
		dir = filepath.Join(os.Getenv("APPDATA"), "Perkeep")
	case os.Getenv("XDG_CONFIG_HOME") != "":
		dir = filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "perkeep")
	default:
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("gateway: %w", err)
		}
		dir = filepath.Join(home, ".config", "perkeep")
	}
	return filepath.Join(dir, "client-config.json"), nil
}

// WriteClientConfig points the Perkeep client config at path to the
// gateway at url, with token, as the "keepsync" server. An existing config
// keeps its other servers and settings, including its identity; the
// gateway becomes the default server unless another one already is. It
// returns whether the gateway is the default.
func WriteClientConfig(path, url, token string) (isDefault bool, err error) {
	conf := map[string]any{}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &conf); err != nil {
			return false, fmt.Errorf("gateway: %s is not a JSON Perkeep client config: %w", path, err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return false, err
	}
	servers, _ := conf["servers"].(map[string]any)
	if servers == nil {
		servers = map[string]any{}
	}
	isDefault = true
	for alias, v := range servers {
		if s, ok := v.(map[string]any); ok && alias != ServerAlias && s["default"] == true {
			isDefault = false
		}
	}
	entry := map[string]any{
		"server": url,
		"auth":   "userpass:" + User + ":" + token,
	}
	if isDefault {
		entry["default"] = true
	}
	servers[ServerAlias] = entry
	conf["servers"] = servers
	out, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(out, '\n'), 0o600); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return false, err
	}
	return isDefault, nil
}
