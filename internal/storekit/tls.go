package storekit

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfigFromEnv builds a client TLS config that trusts the CA in the
// file named by TB_TLS_CA (the benchmark's own CA). It returns nil when the
// variable is not set. The server name is filled in by each driver.
func TLSConfigFromEnv() (*tls.Config, error) {
	path := os.Getenv("TB_TLS_CA")
	if path == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", path)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}
